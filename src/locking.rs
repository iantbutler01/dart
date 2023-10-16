use crate::errors::OptimisticLockCouplingErrorType;
use std::{
    cell::UnsafeCell,
    fmt::Display,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicBool, AtomicU64},
};
use std::{fmt::Debug, sync::atomic::Ordering::*};

/// Result type~
pub type OptimisticLockCouplingResult<T> = Result<T, OptimisticLockCouplingErrorType>;
/// Our data structure, the usage is 'pretty much' same as RwLock
#[derive(Debug)]
pub(crate) struct OptimisticLockCoupling<T: ?Sized> {
    /// 60 bit for version | 1 bit for lock | 1 bit for outdate
    version_lock_outdate: AtomicU64,
    /// guard thread paniced
    poisoned: AtomicBool,
    /// well the data
    data: UnsafeCell<T>,
}

/// Of course Lock could be Send
unsafe impl<T: ?Sized + Send> Send for OptimisticLockCoupling<T> {}
/// Of course Lock could be Sync
unsafe impl<T: ?Sized + Send + Sync> Sync for OptimisticLockCoupling<T> {}

impl<T> OptimisticLockCoupling<T> {
    /// create an instance of OLC
    #[inline(always)]
    pub fn new(t: T) -> Self {
        Self {
            version_lock_outdate: AtomicU64::new(0),
            poisoned: AtomicBool::new(false),
            data: UnsafeCell::new(t),
        }
    }
    /// read transaction
    /// logic should be an inlined closure
    #[inline(always)]
    pub(crate) fn read_txn<F, R>(&self, mut logic: F) -> OptimisticLockCouplingResult<R>
    where
        F: FnMut(&OptimisticLockCouplingReadGuard<T>) -> OptimisticLockCouplingResult<R>,
    {
        'txn: loop {
            match self.read() {
                Ok(guard) => match logic(&guard) {
                    Ok(r) => match guard.try_sync() {
                        Ok(_) => {
                            return Ok(r);
                        }
                        Err(e) => match e {
                            OptimisticLockCouplingErrorType::Poisoned
                            | OptimisticLockCouplingErrorType::Outdated => {
                                return Err(e);
                            }
                            _ => {
                                continue 'txn;
                            }
                        },
                    },
                    Err(e) => match e {
                        OptimisticLockCouplingErrorType::Poisoned
                        | OptimisticLockCouplingErrorType::Outdated => {
                            return Err(e);
                        }
                        _ => {
                            continue 'txn;
                        }
                    },
                },
                Err(e) => match e {
                    OptimisticLockCouplingErrorType::Poisoned
                    | OptimisticLockCouplingErrorType::Outdated => {
                        return Err(e);
                    }
                    _ => {
                        continue 'txn;
                    }
                },
            }
        }
    }
}
impl<T: Sized> From<T> for OptimisticLockCoupling<T> {
    #[inline(always)]
    fn from(t: T) -> Self {
        Self::new(t)
    }
}
impl<T: ?Sized + Default> Default for OptimisticLockCoupling<T> {
    #[inline(always)]
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl<T: ?Sized> OptimisticLockCoupling<T> {
    /// make self outdate
    /// usually used when the container grows and this pointer point to this structure is replaced
    #[inline(always)]
    pub fn make_outdate(&self) {
        self.version_lock_outdate.fetch_or(0b10, Release);
    }
    /// is writter thread dead?
    /// if fail then fail ~
    /// no need extra sync
    #[inline(always)]
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(std::sync::atomic::Ordering::Acquire)
    }
    /// try to aquire the lock but only internal use
    #[inline(always)]
    fn try_lock(&self) -> OptimisticLockCouplingResult<u64> {
        use OptimisticLockCouplingErrorType::*;
        if self.is_poisoned() {
            return Err(Poisoned);
        }
        let version = self.version_lock_outdate.load(Acquire);
        if is_outdate(version) {
            return Err(Outdated);
        }
        if is_locked(version) {
            return Err(Blocked);
        }
        Ok(version)
    }
    /// I suggest you redo the hole function when error occurs
    /// Or just use `read_txn`
    #[inline(always)]
    pub fn read(&self) -> OptimisticLockCouplingResult<OptimisticLockCouplingReadGuard<'_, T>> {
        OptimisticLockCouplingReadGuard::new(self)
    }
    /// get your RAII write guard
    #[inline(always)]
    pub fn write(&self) -> OptimisticLockCouplingResult<OptimisticLockCouplingWriteGuard<'_, T>> {
        use OptimisticLockCouplingErrorType::*;
        let version = self.try_lock()?;
        match self
            .version_lock_outdate
            .compare_exchange(version, version + 0b10, Acquire, Acquire)
        {
            Ok(_) => Ok(OptimisticLockCouplingWriteGuard::new(self)),
            Err(_) => Err(VersionUpdated),
        }
    }
}

#[inline(always)]
fn is_locked(version: u64) -> bool {
    version & 0b10 != 0
}
#[inline(always)]
fn is_outdate(version: u64) -> bool {
    version & 0b1 != 0
}

// ============= reader guard =============== //

/// Usage:
/// after getting the guard you can do what ever you want with Deref
/// but after usage you **MUST** call `try_sync`
/// if fails you must redo the hole function or other sync method to ensure the data you read is correct.
pub struct OptimisticLockCouplingReadGuard<'a, T: ?Sized + 'a> {
    lock: &'a OptimisticLockCoupling<T>,
    version: u64,
}
impl<'a, T: ?Sized> OptimisticLockCouplingReadGuard<'a, T> {
    #[inline(always)]
    pub(crate) fn new(lock: &'a OptimisticLockCoupling<T>) -> OptimisticLockCouplingResult<Self> {
        use crate::locking::OptimisticLockCouplingErrorType::*;
        if lock.is_poisoned() {
            return Err(Poisoned);
        }
        let version = lock.try_lock()?;
        Ok(Self {
            lock: &lock,
            version,
        })
    }
}
impl<T: ?Sized> OptimisticLockCouplingReadGuard<'_, T> {
    /// Consume self return retry or not
    /// suggest to use `read_txn`
    #[inline(always)]
    pub fn try_sync(self) -> OptimisticLockCouplingResult<()> {
        if self.version == self.lock.try_lock()? {
            drop(self);
            Ok(())
        } else {
            use crate::locking::OptimisticLockCouplingErrorType::*;
            Err(VersionUpdated)
        }
    }

    // pub fn upgrade_write_or_error(
    //     self,
    // ) -> OptimisticLockCouplingResult<OptimisticLockCouplingWriteGuard<T>> {
    //     if self.version == self.lock.try_lock()? {
    //         drop(self);
    //         Ok(OptimisticLockCouplingWriteGuard::new(lock))
    //     } else {
    //         use crate::locking::OptimisticLockCouplingErrorType::*;
    //         Err(VersionUpdated)
    //     }
    // }
}
impl<T: ?Sized> Deref for OptimisticLockCouplingReadGuard<'_, T> {
    type Target = T;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.data.get() }
    }
}

impl<T: Debug> Debug for OptimisticLockCouplingReadGuard<'_, T> {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OptimisticLockCouplingReadGuard")
            .field("version", &(self.version >> 2))
            .field("data", self.deref())
            .finish()
    }
}
impl<T: Debug + Display> Display for OptimisticLockCouplingReadGuard<'_, T> {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "OptimisticLockCouplingReadGuard (ver: {}) {}",
            self.version >> 2,
            self.deref()
        ))
    }
}

// ============= writer guard =============== //

/// Only one instance because the data is locked
/// implemented `Deref` and `DerefMut`
/// release the lock on drop
pub struct OptimisticLockCouplingWriteGuard<'a, T: ?Sized + 'a> {
    lock: &'a OptimisticLockCoupling<T>,
}
unsafe impl<T: ?Sized + Sync> Sync for OptimisticLockCouplingWriteGuard<'_, T> {}
impl<T: ?Sized> Deref for OptimisticLockCouplingWriteGuard<'_, T> {
    type Target = T;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.data.get() }
    }
}
impl<T: ?Sized> DerefMut for OptimisticLockCouplingWriteGuard<'_, T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.lock.data.get() }
    }
}
impl<T: ?Sized> Drop for OptimisticLockCouplingWriteGuard<'_, T> {
    #[inline(always)]
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.lock.poisoned.fetch_or(true, Release);
        } else {
            self.lock.version_lock_outdate.fetch_add(0b10, Release);
        }
    }
}
impl<T: Debug> Debug for OptimisticLockCouplingWriteGuard<'_, T> {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OptimisticLockCouplingWriteGuard")
            .field(
                "version",
                &(self.lock.version_lock_outdate.load(Relaxed) >> 2),
            )
            .field("data", self.deref())
            .finish()
    }
}
impl<T: Debug + Display> Display for OptimisticLockCouplingWriteGuard<'_, T> {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "OptimisticLockCouplingWriteGuard (ver: {}) {}",
            self.lock.version_lock_outdate.load(Relaxed) >> 2,
            self.deref()
        ))
    }
}
impl<'a, T: ?Sized> OptimisticLockCouplingWriteGuard<'a, T> {
    #[inline(always)]
    pub(crate) fn new(lock: &'a OptimisticLockCoupling<T>) -> Self {
        Self { lock }
    }
}
