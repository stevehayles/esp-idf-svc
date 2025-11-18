//! High resolution hardware timer based task scheduling
//!
//! Although FreeRTOS provides software timers, these timers have a few
//! limitations:
//!
//! - Maximum resolution is equal to RTOS tick period
//! - Timer callbacks are dispatched from a low-priority task
//!
//! EspTimer is a set of APIs that provides one-shot and periodic timers,
//! microsecond time resolution, and 52-bit range.

use core::num::NonZeroU32;
use core::time::Duration;
use core::{ffi, ptr};

extern crate alloc;
use alloc::boxed::Box;
use alloc::sync::Arc;

use esp_idf_hal::task::asynch::Notification;

use crate::sys::*;

use ::log::debug;

#[cfg(esp_idf_esp_timer_supports_isr_dispatch_method)]
pub use isr::*;

use crate::handle::RawHandle;

struct UnsafeCallback<'a>(*mut Box<dyn FnMut() + Send + 'a>);

impl<'a> UnsafeCallback<'a> {
    fn from(boxed: &mut Box<dyn FnMut() + Send + 'a>) -> Self {
        Self(boxed)
    }

    unsafe fn from_ptr(ptr: *mut ffi::c_void) -> Self {
        Self(ptr as *mut _)
    }

    fn as_ptr(&self) -> *mut ffi::c_void {
        self.0 as *mut _
    }

    unsafe fn call(&self) {
        let reference = self.0.as_mut().unwrap();

        (reference)();
    }
}

pub struct EspTimer<'a> {
    handle: esp_timer_handle_t,
    _callback: Box<dyn FnMut() + Send + 'a>,
}

impl EspTimer<'_> {
    pub fn is_scheduled(&self) -> Result<bool, EspError> {
        Ok(unsafe { esp_timer_is_active(self.handle) })
    }

    pub fn cancel(&self) -> Result<bool, EspError> {
        let res = unsafe { esp_timer_stop(self.handle) };

        Ok(res != ESP_OK)
    }

    pub fn after(&self, duration: Duration) -> Result<(), EspError> {
        self.cancel()?;

        esp!(unsafe { esp_timer_start_once(self.handle, duration.as_micros() as _) })?;

        Ok(())
    }

    pub fn every(&self, duration: Duration) -> Result<(), EspError> {
        self.cancel()?;

        esp!(unsafe { esp_timer_start_periodic(self.handle, duration.as_micros() as _) })?;

        Ok(())
    }

    extern "C" fn handle(arg: *mut ffi::c_void) {
        if crate::hal::interrupt::active() {
            #[cfg(esp_idf_esp_timer_supports_isr_dispatch_method)]
            {
                let signaled = crate::hal::interrupt::with_isr_yield_signal(move || unsafe {
                    UnsafeCallback::from_ptr(arg).call();
                });

                if signaled {
                    unsafe {
                        crate::sys::esp_timer_isr_dispatch_need_yield();
                    }
                }
            }

            #[cfg(not(esp_idf_esp_timer_supports_isr_dispatch_method))]
            {
                unreachable!();
            }
        } else {
            unsafe {
                UnsafeCallback::from_ptr(arg).call();
            }
        }
    }
}

unsafe impl Send for EspTimer<'_> {}

impl Drop for EspTimer<'_> {
    fn drop(&mut self) {
        self.cancel().unwrap();

        while unsafe { esp_timer_delete(self.handle) } != ESP_OK {
            // Timer is still running, busy-loop
        }

        debug!("Timer dropped");
    }
}

impl RawHandle for EspTimer<'_> {
    type Handle = esp_timer_handle_t;

    fn handle(&self) -> Self::Handle {
        self.handle
    }
}

pub struct EspAsyncTimer {
    timer: EspTimer<'static>,
    notification: Arc<Notification>,
}

impl EspAsyncTimer {
    pub async fn after(&mut self, duration: Duration) -> Result<(), EspError> {
        self.timer.cancel()?;

        self.notification.reset();
        self.timer.after(duration)?;

        self.notification.wait().await;

        Ok(())
    }

    pub fn every(&mut self, duration: Duration) -> Result<&'_ mut Self, EspError> {
        self.timer.cancel()?;

        self.notification.reset();
        self.timer.every(duration)?;

        Ok(self)
    }

    pub async fn tick(&mut self) -> Result<(), EspError> {
        self.notification.wait().await;

        Ok(())
    }
}

impl embedded_hal_async::delay::DelayNs for EspAsyncTimer {
    async fn delay_ns(&mut self, ns: u32) {
        EspAsyncTimer::after(self, Duration::from_micros(ns as _))
            .await
            .unwrap();
    }

    async fn delay_ms(&mut self, ms: u32) {
        EspAsyncTimer::after(self, Duration::from_millis(ms as _))
            .await
            .unwrap();
    }
}

pub trait EspTimerServiceType {
    fn is_isr() -> bool;
}

#[derive(Clone, Debug)]
pub struct Task;

impl EspTimerServiceType for Task {
    fn is_isr() -> bool {
        false
    }
}

pub struct EspTimerService<T>(T)
where
    T: EspTimerServiceType;

impl<T> EspTimerService<T>
where
    T: EspTimerServiceType,
{
    pub fn now(&self) -> Duration {
        Duration::from_micros(unsafe { esp_timer_get_time() as _ })
    }

    pub fn timer<F>(&self, callback: F) -> Result<EspTimer<'static>, EspError>
    where
        F: FnMut() + Send + 'static,
    {
        self.internal_timer(callback, false)
    }

    /// Same as `timer` but does not wake the device from light sleep.
    pub fn timer_nowake<F>(&self, callback: F) -> Result<EspTimer<'static>, EspError>
    where
        F: FnMut() + Send + 'static,
    {
        self.internal_timer(callback, true)
    }

    pub fn timer_async(&self) -> Result<EspAsyncTimer, EspError> {
        self.internal_timer_async(false)
    }

    /// Same as `timer_async` but does not wake the device from light sleep.
    pub fn timer_async_nowake(&self) -> Result<EspAsyncTimer, EspError> {
        self.internal_timer_async(true)
    }

    /// # Safety
    ///
    /// This method - in contrast to method `timer` - allows the user to pass
    /// a non-static callback/closure. This enables users to borrow
    /// - in the closure - variables that live on the stack - or more generally - in the same
    ///   scope where the service is created.
    ///
    /// HOWEVER: care should be taken NOT to call `core::mem::forget()` on the service,
    /// as that would immediately lead to an UB (crash).
    /// Also note that forgetting the service might happen with `Rc` and `Arc`
    /// when circular references are introduced: https://github.com/rust-lang/rust/issues/24456
    ///
    /// The reason is that the closure is actually sent to a hidden ESP IDF thread.
    /// This means that if the service is forgotten, Rust is free to e.g. unwind the stack
    /// and the closure now owned by this other thread will end up with references to variables that no longer exist.
    ///
    /// The destructor of the service takes care - prior to the service being dropped and e.g.
    /// the stack being unwind - to remove the closure from the hidden thread and destroy it.
    /// Unfortunately, when the service is forgotten, the un-subscription does not happen
    /// and invalid references are left dangling.
    ///
    /// This "local borrowing" will only be possible to express in a safe way once/if `!Leak` types
    /// are introduced to Rust (i.e. the impossibility to "forget" a type and thus not call its destructor).
    pub unsafe fn timer_nonstatic<'a, F>(&self, callback: F) -> Result<EspTimer<'a>, EspError>
    where
        F: FnMut() + Send + 'a,
    {
        self.internal_timer(callback, false)
    }

    /// # Safety
    ///
    /// Same as `timer_nonstatic` but does not wake the device from light sleep.
    pub unsafe fn timer_nonstatic_nowake<'a, F>(
        &self,
        callback: F,
    ) -> Result<EspTimer<'a>, EspError>
    where
        F: FnMut() + Send + 'a,
    {
        self.internal_timer(callback, true)
    }

    fn internal_timer<'a, F>(
        &self,
        callback: F,
        skip_unhandled_events: bool,
    ) -> Result<EspTimer<'a>, EspError>
    where
        F: FnMut() + Send + 'a,
    {
        let mut handle: esp_timer_handle_t = ptr::null_mut();

        let boxed_callback: Box<dyn FnMut() + Send + 'a> = Box::new(callback);

        let mut callback = Box::new(boxed_callback);
        let unsafe_callback = UnsafeCallback::from(&mut callback);

        #[cfg(esp_idf_esp_timer_supports_isr_dispatch_method)]
        let dispatch_method = if T::is_isr() {
            esp_timer_dispatch_t_ESP_TIMER_ISR
        } else {
            esp_timer_dispatch_t_ESP_TIMER_TASK
        };

        #[cfg(not(esp_idf_esp_timer_supports_isr_dispatch_method))]
        let dispatch_method = esp_timer_dispatch_t_ESP_TIMER_TASK;

        esp!(unsafe {
            esp_timer_create(
                &esp_timer_create_args_t {
                    callback: Some(EspTimer::handle),
                    name: b"rust\0" as *const _ as *const _, // TODO
                    arg: unsafe_callback.as_ptr(),
                    dispatch_method,
                    skip_unhandled_events,
                },
                &mut handle as *mut _,
            )
        })?;

        Ok(EspTimer {
            handle,
            _callback: callback,
        })
    }

    fn internal_timer_async(&self, skip_unhandled_events: bool) -> Result<EspAsyncTimer, EspError> {
        let notification = Arc::new(Notification::new());

        let timer = {
            let notification = Arc::downgrade(&notification);

            self.internal_timer(
                move || {
                    if let Some(notification) = notification.upgrade() {
                        notification.notify(NonZeroU32::new(1).unwrap());
                    }
                },
                skip_unhandled_events,
            )?
        };

        Ok(EspAsyncTimer {
            timer,
            notification,
        })
    }
}

pub type EspTaskTimerService = EspTimerService<Task>;

impl EspTimerService<Task> {
    pub fn new() -> Result<Self, EspError> {
        Ok(Self(Task))
    }
}

impl<T> Clone for EspTimerService<T>
where
    T: EspTimerServiceType + Clone,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[cfg(esp_idf_esp_timer_supports_isr_dispatch_method)]
mod isr {
    use crate::sys::EspError;

    #[derive(Clone, Debug)]
    pub struct ISR;

    impl super::EspTimerServiceType for ISR {
        fn is_isr() -> bool {
            true
        }
    }

    pub type EspISRTimerService = super::EspTimerService<ISR>;

    impl EspISRTimerService {
        /// # Safety
        /// TODO
        pub unsafe fn new() -> Result<Self, EspError> {
            Ok(Self(ISR))
        }
    }
}

#[cfg(feature = "embassy-time-driver")]
mod timer_wheel {
    use core::cmp::{min, Ordering};
    use core::task::Waker;

    use heapless::Vec;

    #[derive(Debug)]
    struct Timer {
        /// Absolute deadline in microseconds.
        deadline: u64,
        waker: Waker,
    }

    impl PartialEq for Timer {
        fn eq(&self, other: &Self) -> bool {
            self.deadline == other.deadline && self.waker.will_wake(&other.waker)
        }
    }
    impl Eq for Timer {}

    impl PartialOrd for Timer {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            self.deadline.partial_cmp(&other.deadline)
        }
    }
    impl Ord for Timer {
        fn cmp(&self, other: &Self) -> Ordering {
            self.deadline.cmp(&other.deadline)
        }
    }

    /// Single-level timer wheel.
    ///
    /// - `BUCKETS`: number of buckets in the wheel
    /// - `PER_BUCKET`: max timers per bucket
    /// - `RES_US`: resolution per bucket in microseconds
    #[derive(Debug)]
    pub struct TimerWheel<const BUCKETS: usize, const PER_BUCKET: usize, const RES_US: u64> {
        /// Buckets of timers; each bucket holds timers whose deadline falls into
        /// some `[k*RES_US, (k+1)*RES_US)` window modulo `BUCKETS * RES_US`.
        buckets: [Vec<Timer, PER_BUCKET>; BUCKETS],

        /// Current "tick" (time / RES_US) we've advanced to.
        current_tick: u64,
        initialized: bool,
    }

    impl<const BUCKETS: usize, const PER_BUCKET: usize, const RES_US: u64>
        TimerWheel<BUCKETS, PER_BUCKET, RES_US>
    {
        /// Create a new wheel, with an initial "now" in microseconds.
        /// This lets us align `current_tick` to the real time base.
        pub fn new() -> Self {
            // Runtime assertion: BUCKETS should be power of 2 for optimal modulo
            // TODO: Const can't be used from an outer type so not sure how to check at compile time...
            assert!(BUCKETS.is_power_of_two(), "BUCKETS must be a power of 2");

            Self {
                buckets: core::array::from_fn(|_| Vec::new()),
                current_tick: 0,
                initialized: false,
            }
        }

        /// Compute bucket index for an absolute deadline.
        /// Uses bitwise AND for modulo when BUCKETS is power of 2.
        #[inline]
        fn bucket_index(deadline: u64) -> usize {
            // Integer division truncates: each bucket covers RES_US microseconds.
            let tick = deadline / RES_US;
            // Fast modulo for power-of-2 BUCKETS
            (tick as usize) & (BUCKETS - 1)
        }

        /// Internal helper: ensure the wheel is initialized with a real timestamp.
        #[inline]
        fn ensure_init(&mut self, now_us: u64) {
            if !self.initialized {
                self.current_tick = now_us / RES_US;
                self.initialized = true;
            }
        }

        /// Schedules a task to run at `at` (absolute µs timestamp).
        ///
        /// Returns `true` if the *earliest deadline in the wheel* might have changed,
        /// in which case the caller should reprogram the underlying hardware timer.
        pub fn schedule_wake(&mut self, at: u64, waker: &Waker) -> bool {
            // lazy init
            self.ensure_init(at);

            // Fast path: see if this waker already has a timer; if so, update deadline.
            // We search all buckets; if you want, you can keep a side map<waker, bucket>
            // later for O(1) lookup, but this keeps things simple and small.
            let mut found = None;

            'outer: for (b_idx, bucket) in self.buckets.iter_mut().enumerate() {
                for (t_idx, timer) in bucket.iter_mut().enumerate() {
                    if timer.waker.will_wake(waker) {
                        found = Some((b_idx, t_idx));
                        // Update deadline; we may want to move to another bucket.
                        timer.deadline = at;
                        break 'outer;
                    }
                }
            }

            if let Some((old_bucket_idx, timer_idx)) = found {
                let new_bucket_idx = Self::bucket_index(at);

                // If the bucket index changed, move the timer into the proper bucket.
                if new_bucket_idx != old_bucket_idx {
                    let mut timer = self.buckets[old_bucket_idx].swap_remove(timer_idx);
                    timer.deadline = at;

                    // Try to insert into the new bucket.
                    self.insert_into_bucket(new_bucket_idx, timer);
                }

                // Conservative: earliest deadline might have changed.
                // For small BUCKETS this is cheap enough.
                return true;
            }

            // Not found: create a new timer.
            let timer = Timer {
                deadline: at,
                waker: waker.clone(),
            };

            let bucket_idx = Self::bucket_index(at);
            self.insert_into_bucket(bucket_idx, timer)
        }

        /// Helper: insert into a bucket, evicting the *latest* timer in that bucket if needed.
        ///
        /// Returns `true` if earliest deadline in the wheel might have changed.
        #[inline]
        fn insert_into_bucket(&mut self, bucket_idx: usize, mut timer: Timer) -> bool {
            let bucket = &mut self.buckets[bucket_idx];

            if bucket.len() < PER_BUCKET {
                // Space available; just push.
                // We *could* keep bucket sorted by deadline, but it's not necessary:
                // we always check `deadline <= now` before firing.
                bucket.push(timer).ok().unwrap();
                return true;
            }

            // Bucket full: evict the timer with the *latest* deadline in that bucket.
            // This is much fairer than "pop arbitrary last" like the old queue.
            let mut worst_idx = 0;
            let mut worst_deadline = bucket[0].deadline;

            for (i, t) in bucket.iter().enumerate().skip(1) {
                if t.deadline > worst_deadline {
                    worst_deadline = t.deadline;
                    worst_idx = i;
                }
            }

            // If the new timer is later than the worst, just wake it immediately
            // (it's "least urgent"), and keep the existing timers.
            if timer.deadline >= worst_deadline {
                timer.waker.wake_by_ref();
                return false;
            }

            // Otherwise, evict the worst, wake that one early, and insert the new one.
            let evicted = bucket.swap_remove(worst_idx);
            evicted.waker.wake_by_ref();

            bucket.push(timer).ok().unwrap();

            true
        }

        /// Dequeues expired timers and returns the next alarm time as an absolute µs timestamp.
        ///
        /// If there are no timers left, returns `u64::MAX`.
        pub fn next_expiration(&mut self, now_us: u64) -> u64 {
            self.ensure_init(now_us);

            let now_tick = now_us / RES_US;

            // 1. Advance wheel tick-by-tick up to now_tick, firing expired timers.
            while self.current_tick <= now_tick {
                let bucket_idx = (self.current_tick as usize) % BUCKETS;
                let bucket = &mut self.buckets[bucket_idx];

                let mut i = 0;
                while i < bucket.len() {
                    if bucket[i].deadline <= now_us {
                        let timer = bucket.swap_remove(i);
                        timer.waker.wake_by_ref();
                    } else {
                        i += 1;
                    }
                }

                self.current_tick += 1;
            }

            // 2. Find earliest future deadline across all buckets.
            let mut next_alarm = u64::MAX;

            for bucket in &self.buckets {
                for timer in bucket.iter() {
                    next_alarm = min(next_alarm, timer.deadline);
                }
            }

            next_alarm
        }
    }
}

/// This module is used to provide a time driver for the `embassy-time` crate.
///
/// The minimum provided resolution is ~ 20-30us when the CPU is at top speed of 240MHz
/// (https://docs.espressif.com/projects/esp-idf/en/v5.4/esp32/api-reference/system/esp_timer.html#timeout-value-limits)
///
/// The tick-rate is 1MHz (i.e. 1 tick is 1us).
#[cfg(feature = "embassy-time-driver")]
pub mod embassy_time_driver {
    use core::task::Waker;
    use std::sync::OnceLock;

    use ::embassy_time_driver::Driver;

    use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
    use embassy_sync::blocking_mutex::Mutex as CsMutex;

    use super::timer_wheel::TimerWheel;

    use crate::timer::*;

    static TIMER_SERVICE: OnceLock<EspTaskTimerService> = OnceLock::new();

    // Example tuning: 32 buckets, 4 timers per bucket, 100 µs resolution
    pub type InnerWheel = TimerWheel<32, 4, 100>;

    #[derive(Debug)]
    struct Queue {
        wheel: InnerWheel,
    }

    impl Queue {
        pub fn new() -> Self {
            Self {
                wheel: InnerWheel::new(),
            }
        }

        pub fn schedule_wake(&mut self, at: u64, waker: &Waker) -> bool {
            self.wheel.schedule_wake(at, waker)
        }

        pub fn next_expiration(&mut self, now: u64) -> u64 {
            self.wheel.next_expiration(now)
        }
    }

    struct EspDriverInner {
        queue: Option<Queue>,
        timer: Option<EspTimer<'static>>,
    }

    impl EspDriverInner {
        /// Current time in microseconds as a 64-bit monotonic counter.
        #[inline(always)]
        fn now() -> u64 {
            unsafe { esp_timer_get_time() as _ }
        }

        fn queue_mut(&mut self) -> &mut Queue {
            if self.queue.is_none() {
                // lazy runtime init (legal because this isn't const code)
                self.queue = Some(Queue::new());
            }
            self.queue.as_mut().unwrap()
        }

        fn schedule_next_expiration(&mut self) {
            /// End of epoch minus one day
            const MAX_SAFE_TIMEOUT_US: u64 = u64::MAX - 24 * 60 * 60 * 1000 * 1000;

            loop {
                let now = Self::now();

                let next_at = {
                    let queue = self.queue_mut();
                    queue.next_expiration(now)
                };

                let timer = self
                    .timer
                    .as_mut()
                    .expect("timer must be created before scheduling");

                // `next_at == u64::MAX` means "no timers" in the integrated queue.
                if next_at == u64::MAX {
                    // Leave the timer disarmed; nothing to do.
                    break;
                }

                if now < next_at {
                    let after = next_at - now;

                    if after <= MAX_SAFE_TIMEOUT_US {
                        // Why?
                        // The ESP-IDF Timer API does not have a `Timer::at` method so we have to call it with
                        // `Timer::after(next_at - now)` instead. The problem is - even though the ESP IDF
                        // Timer API does not have a `Timer::at` method - _internally_ it takes our `next_at - now`,
                        // adds to it a **newer** "now" and sets this as the moment in time when the timer should trigger.
                        //
                        // Consider what would happen if we call `Timer::after(u64::MAX - now)`:
                        // The result would be something like `u64::MAX - now + (now + 1)` which would silently overflow and
                        // trigger the timer after 1us:
                        // https://github.com/espressif/esp-idf/blob/b5ac4fbdf9e9fb320bb0a98ee4fbaa18f8566f37/components/esp_timer/src/esp_timer.c#L188
                        //
                        // To workaround this problem, we make sure to never call `Timer::after(ms)` with `ms` greater than `MAX_SAFE_TIMEOUT_US`
                        // (i.e. the end of epoch - one day).
                        //
                        // Thus, even if we are un-scheduled between the calculation of our own `now` and the driver's newer `now`,
                        // there is one extra **day** of millis to accomodate for the potential overflow. If the overflow does happen still
                        // (which is kinda unthinkable given the time scales we are working with), the timer will re-trigger immediately,
                        // but hopefully on the next (or next after next and so on) re-trigger, we won't have the overflow anymore.
                        timer.after(Duration::from_micros(after)).unwrap();
                    }

                    break;
                }
            }
        }
    }

    struct EspDriver {
        inner: CsMutex<CriticalSectionRawMutex, EspDriverInner>,
    }

    impl EspDriver {
        const fn new() -> Self {
            Self {
                inner: CsMutex::new(EspDriverInner {
                    queue: None,
                    timer: None,
                }),
            }
        }
    }

    unsafe impl Send for EspDriver {}
    unsafe impl Sync for EspDriver {}

    impl Driver for EspDriver {
        #[inline(always)]
        fn now(&self) -> u64 {
            EspDriverInner::now()
        }

        fn schedule_wake(&self, at: u64, waker: &Waker) {
            let service = TIMER_SERVICE.get_or_init(|| EspTaskTimerService::new().unwrap());

            // SAFETY: `lock_mut` is unsafe because calling it *re-entrantly* on the same
            // mutex would create simultaneous &mut references to the same data, which
            // violates Rust’s aliasing rules. In this driver we never call `lock_mut`
            // re-entrantly: `schedule_wake()` runs in the executor task, while the
            // timer callback runs in the esp_timer task, and these contexts never invoke
            // `lock_mut` while another `lock_mut` closure is active on the same thread.
            // The raw mutex (CriticalSectionRawMutex) provides mutual exclusion across
            // tasks, ensuring only one mutable borrow exists at a time. Therefore, this
            // call cannot create overlapping &mut borrows and is sound.
            unsafe {
                self.inner.lock_mut(|state| {
                    if state.timer.is_none() {
                        // Driver is statically allocated, so this is safe.
                        let static_self: &'static Self = core::mem::transmute(self);

                        state.timer = Some(
                            service
                                .timer(move || {
                                    static_self.inner.lock_mut(|s| {
                                        s.schedule_next_expiration();
                                    });
                                })
                                .unwrap(),
                        );
                    }

                    if state.queue_mut().schedule_wake(at, waker) {
                        state.schedule_next_expiration();
                    }
                })
            };
        }
    }

    pub type LinkWorkaround = [*mut (); 2];

    #[used]
    static mut __INTERNAL_REFERENCE: LinkWorkaround = [
        _embassy_time_now as *mut _,
        _embassy_time_schedule_wake as *mut _,
    ];

    pub fn link() -> LinkWorkaround {
        unsafe { __INTERNAL_REFERENCE }
    }

    ::embassy_time_driver::time_driver_impl!(static DRIVER: EspDriver = EspDriver::new());
}
