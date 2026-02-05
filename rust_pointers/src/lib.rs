//! Rust Pointer Exercises
//!
//! Run tests with: cargo test
//! Run specific task: cargo test task1
//! See rust_pointers/rust_pointers.md for more info.

#![allow(unused)]

use std::ptr::NonNull;

// ============================================================================
// Task 1: Integer to Raw Pointer
// ============================================================================
//
// Convert a memory address (usize) to a raw pointer.
// This is common in OS/embedded code for memory-mapped I/O.
//
// Pattern: `address as *mut T` or `address as *const T`

/// Convert address to a mutable raw pointer to u8
pub fn addr_to_ptr_mut(addr: usize) -> *mut u8 {
    addr as *mut u8
}

/// Convert address to a const raw pointer to u32
pub fn addr_to_ptr_const(addr: usize) -> *const u32 {
    addr as *const u32
}

#[cfg(test)]
mod task1 {
    use super::*;

    #[test]
    fn test_addr_to_mut_ptr() {
        let addr: usize = 0x1000;
        let ptr = addr_to_ptr_mut(addr);
        assert_eq!(ptr as usize, 0x1000);
    }

    #[test]
    fn test_addr_to_const_ptr() {
        let addr: usize = 0x2000;
        let ptr = addr_to_ptr_const(addr);
        assert_eq!(ptr as usize, 0x2000);
    }
}

// ============================================================================
// Task 2: Reference to Raw Pointer
// ============================================================================
//
// Convert a Rust reference to a raw pointer.
// Useful when you need to escape the borrow checker temporarily.
//
// Pattern: `&x as *const T` or `&mut x as *mut T`

/// Convert a shared reference to a const raw pointer
pub fn ref_to_const_ptr<T>(r: &T) -> *const T {
    r as *const T
}

/// Convert a mutable reference to a mutable raw pointer
pub fn ref_to_mut_ptr<T>(r: &mut T) -> *mut T {
    r as *mut T
}

#[cfg(test)]
mod task2 {
    use super::*;

    #[test]
    fn test_ref_to_const_ptr() {
        let x = 42i32;
        let ptr = ref_to_const_ptr(&x);
        assert_eq!(ptr, &x as *const i32);
    }

    #[test]
    fn test_ref_to_mut_ptr() {
        let mut x = 42i32;
        let ptr = ref_to_mut_ptr(&mut x);
        assert_eq!(ptr, &mut x as *mut i32);
    }
}

// ============================================================================
// Task 3: Dereference Raw Pointer
// ============================================================================
//
// Read a value through a raw pointer. Requires unsafe.
//
// Pattern: unsafe { *ptr }

/// Read the value at ptr. Caller must ensure ptr is valid and aligned.
///
/// # Safety
/// ptr must point to a valid, initialized T
pub unsafe fn read_through_ptr<T: Copy>(ptr: *const T) -> T {
    *ptr
}

/// Write a value through a mutable raw pointer.
///
/// # Safety
/// ptr must point to valid, writable memory for T
pub unsafe fn write_through_ptr<T>(ptr: *mut T, value: T) {
    *ptr = value
}

#[cfg(test)]
mod task3 {
    use super::*;

    #[test]
    fn test_read_through_ptr() {
        let x = 123i32;
        let ptr = &x as *const i32;
        let value = unsafe { read_through_ptr(ptr) };
        assert_eq!(value, 123);
    }

    #[test]
    fn test_write_through_ptr() {
        let mut x = 0i32;
        let ptr = &mut x as *mut i32;
        unsafe { write_through_ptr(ptr, 456) };
        assert_eq!(x, 456);
    }
}

// ============================================================================
// Task 4: Dereference-Reborrow Pattern
// ============================================================================
//
// Convert a raw pointer back to a reference.
// Common pattern: `&*ptr` or `&mut *ptr`
//
// This is what happens in: buffer: unsafe { &mut *(0xb8000 as *mut Buffer) }

/// Convert a const raw pointer to a shared reference.
///
/// # Safety
/// ptr must be valid, aligned, and point to initialized data.
/// The returned reference must not outlive the data.
pub unsafe fn ptr_to_ref<'a, T>(ptr: *const T) -> &'a T {
    &*ptr
}

/// Convert a mutable raw pointer to a mutable reference.
///
/// # Safety
/// ptr must be valid, aligned, point to initialized data,
/// and no other references to the data may exist.
pub unsafe fn ptr_to_mut_ref<'a, T>(ptr: *mut T) -> &'a mut T {
    &mut *ptr
}

#[cfg(test)]
mod task4 {
    use super::*;

    #[test]
    fn test_ptr_to_ref() {
        let x = 789i32;
        let ptr = &x as *const i32;
        let r: &i32 = unsafe { ptr_to_ref(ptr) };
        assert_eq!(*r, 789);
    }

    #[test]
    fn test_ptr_to_mut_ref() {
        let mut x = 0i32;
        let ptr = &mut x as *mut i32;
        let r: &mut i32 = unsafe { ptr_to_mut_ref(ptr) };
        *r = 999;
        assert_eq!(x, 999);
    }
}

// ============================================================================
// Task 5: Field Access Through Pointer
// ============================================================================
//
// Access a struct field through a raw pointer.
// Pattern: (*ptr).field
//
// This is what happens in: (*node.as_ptr()).elem

pub struct Point {
    pub x: i32,
    pub y: i32,
}

/// Read the x field from a Point through a raw pointer.
///
/// # Safety
/// ptr must point to a valid Point
pub unsafe fn read_x(ptr: *const Point) -> i32 {
    (*ptr).x
}

/// Set the y field of a Point through a raw pointer.
///
/// # Safety
/// ptr must point to a valid, writable Point
pub unsafe fn write_y(ptr: *mut Point, value: i32) {
    (*ptr).y = value
}

#[cfg(test)]
mod task5 {
    use super::*;

    #[test]
    fn test_read_field() {
        let p = Point { x: 10, y: 20 };
        let ptr = &p as *const Point;
        let x = unsafe { read_x(ptr) };
        assert_eq!(x, 10);
    }

    #[test]
    fn test_write_field() {
        let mut p = Point { x: 10, y: 20 };
        let ptr = &mut p as *mut Point;
        unsafe { write_y(ptr, 99) };
        assert_eq!(p.y, 99);
    }
}

// ============================================================================
// Task 6: NonNull Basics
// ============================================================================
//
// NonNull<T> is a non-null pointer type. Common in data structures.
// Key methods:
// - NonNull::new(ptr) -> Option<NonNull<T>>
// - NonNull::new_unchecked(ptr) -> NonNull<T>  (unsafe)
// - nonnull.as_ptr() -> *mut T

/// Wrap a raw pointer in NonNull, returning None if null.
pub fn wrap_in_nonnull<T>(ptr: *mut T) -> Option<NonNull<T>> {
    NonNull::new(ptr)
}

/// Get the raw pointer from a NonNull.
pub fn unwrap_nonnull<T>(nn: NonNull<T>) -> *mut T {
    nn.as_ptr()
}

/// Read a value through NonNull.
///
/// # Safety
/// The NonNull must point to valid, initialized data.
pub unsafe fn read_via_nonnull<T: Copy>(nn: NonNull<T>) -> T {
    nn.read()
}

#[cfg(test)]
mod task6 {
    use super::*;

    #[test]
    fn test_wrap_nonnull() {
        let mut x = 42i32;
        let ptr = &mut x as *mut i32;
        let nn = wrap_in_nonnull(ptr);
        assert!(nn.is_some());

        let null_ptr: *mut i32 = std::ptr::null_mut();
        let nn_null = wrap_in_nonnull(null_ptr);
        assert!(nn_null.is_none());
    }

    #[test]
    fn test_unwrap_nonnull() {
        let mut x = 42i32;
        let ptr = &mut x as *mut i32;
        let nn = NonNull::new(ptr).unwrap();
        let raw = unwrap_nonnull(nn);
        assert_eq!(raw, ptr);
    }

    #[test]
    fn test_read_via_nonnull() {
        let x = 123i32;
        let ptr = &x as *const i32 as *mut i32; // const to mut for NonNull
        let nn = NonNull::new(ptr).unwrap();
        let value = unsafe { read_via_nonnull(nn) };
        assert_eq!(value, 123);
    }
}

// ============================================================================
// Task 7: Box::into_raw and Box::from_raw
// ============================================================================
//
// Transfer ownership between Box and raw pointer.
// - Box::into_raw(b) -> *mut T  (consumes Box, leaks if not reclaimed)
// - Box::from_raw(ptr) -> Box<T>  (takes ownership back)

/// Convert a Box to a raw pointer (ownership transferred out).
pub fn box_to_raw<T>(b: Box<T>) -> *mut T {
    Box::into_raw(b)
}

/// Convert a raw pointer back to a Box (takes ownership).
///
/// # Safety
/// ptr must have come from Box::into_raw and not been freed.
pub unsafe fn raw_to_box<T>(ptr: *mut T) -> Box<T> {
    Box::from_raw(ptr)
}

#[cfg(test)]
mod task7 {
    use super::*;

    #[test]
    fn test_box_roundtrip() {
        let b = Box::new(42i32);
        let ptr = box_to_raw(b);
        // b is now consumed, ptr owns the memory

        let b2 = unsafe { raw_to_box(ptr) };
        assert_eq!(*b2, 42);
        // b2 will be dropped here, freeing memory
    }

    #[test]
    fn test_box_to_raw_address() {
        let b = Box::new(String::from("hello"));
        let expected_addr = &*b as *const String as usize;
        let ptr = box_to_raw(b);
        assert_eq!(ptr as usize, expected_addr);
        // Clean up
        let _ = unsafe { raw_to_box(ptr) };
    }
}

// ============================================================================
// Task 8: Combined - Linked List Node Access
// ============================================================================
//
// Put it all together: access a field through NonNull.
// Pattern: &(*node.as_ptr()).field
//
// This is the pattern from too-many-lists.

pub struct Node<T> {
    pub elem: T,
    pub next: Option<NonNull<Node<T>>>,
}

/// Get a reference to the element in a node.
///
/// # Safety
/// nn must point to a valid Node
pub unsafe fn get_elem<'a, T>(nn: NonNull<Node<T>>) -> &'a T {
    &(*nn.as_ptr()).elem
}

/// Get a mutable reference to the element in a node.
///
/// # Safety
/// nn must point to a valid Node, and no other references may exist.
pub unsafe fn get_elem_mut<'a, T>(nn: NonNull<Node<T>>) -> &'a mut T {
    &mut (*nn.as_ptr()).elem
}

/// Set the next pointer of a node.
///
/// # Safety
/// nn must point to a valid Node
pub unsafe fn set_next<T>(nn: NonNull<Node<T>>, next: Option<NonNull<Node<T>>>) {
    (*nn.as_ptr()).next = next
}

#[cfg(test)]
mod task8 {
    use super::*;

    #[test]
    fn test_get_elem() {
        let node = Box::new(Node {
            elem: 42,
            next: None,
        });
        let ptr = Box::into_raw(node);
        let nn = NonNull::new(ptr).unwrap();

        let elem = unsafe { get_elem(nn) };
        assert_eq!(*elem, 42);

        // Clean up
        let _ = unsafe { Box::from_raw(ptr) };
    }

    #[test]
    fn test_get_elem_mut() {
        let node = Box::new(Node {
            elem: 0,
            next: None,
        });
        let ptr = Box::into_raw(node);
        let nn = NonNull::new(ptr).unwrap();

        let elem = unsafe { get_elem_mut(nn) };
        *elem = 99;

        let node = unsafe { Box::from_raw(ptr) };
        assert_eq!(node.elem, 99);
    }

    #[test]
    fn test_set_next() {
        let node1 = Box::new(Node {
            elem: 1,
            next: None,
        });
        let node2 = Box::new(Node {
            elem: 2,
            next: None,
        });

        let ptr1 = Box::into_raw(node1);
        let ptr2 = Box::into_raw(node2);
        let nn1 = NonNull::new(ptr1).unwrap();
        let nn2 = NonNull::new(ptr2).unwrap();

        unsafe { set_next(nn1, Some(nn2)) };

        let node1 = unsafe { Box::from_raw(ptr1) };
        assert!(node1.next.is_some());
        assert_eq!(node1.next.unwrap().as_ptr(), ptr2);

        // Clean up node2
        let _ = unsafe { Box::from_raw(ptr2) };
    }
}

// ============================================================================
// Task 9: The Phantom Aliasing Bug
// ============================================================================
//
// Your colleague wrote a "fast" slice splitter using raw pointers.
// It compiles and sometimes works. But it's unsound.
//
// The bug: creating two &mut references to overlapping memory regions is UB,
// even if you never actually access the overlap. The compiler assumes &mut
// is exclusive and may optimize based on that.
//
// Your task: implement a predicate that validates whether a split is safe.

/// This is your colleague's buggy function. DON'T USE IT.
/// It creates two mutable slices from a single slice using raw pointers.
///
/// BUG: If the regions overlap, this is undefined behavior!
#[allow(dead_code)]
unsafe fn buggy_split_mut<T>(
    slice: &mut [T],
    start1: usize,
    len1: usize,
    start2: usize,
    len2: usize,
) -> (&mut [T], &mut [T]) {
    let ptr = slice.as_mut_ptr();
    let slice1 = std::slice::from_raw_parts_mut(ptr.add(start1), len1);
    let slice2 = std::slice::from_raw_parts_mut(ptr.add(start2), len2);
    (slice1, slice2)
}

/// Returns true if splitting a slice of `total_len` into two regions
/// [start1..start1+len1) and [start2..start2+len2) would be safe.
///
/// Safe means:
/// - Both regions are within bounds
/// - The regions do not overlap (adjacent is OK, overlapping is not)
///
/// Think carefully about edge cases:
/// - Zero-length regions: are they ever a problem?
/// - What if start1 == start2 but both lengths are 0?
/// - What about integer overflow?
pub fn is_safe_split(
    start1: usize,
    len1: usize,
    start2: usize,
    len2: usize,
    total_len: usize,
) -> bool {
    todo!()
}

#[cfg(test)]
mod task9 {
    use super::*;

    #[test]
    fn test_non_overlapping() {
        // [0,1,2] and [3,4,5] in a slice of 6
        assert!(is_safe_split(0, 3, 3, 3, 6));
    }

    #[test]
    fn test_overlapping() {
        // [0,1,2,3] and [2,3,4,5] overlap at indices 2,3
        assert!(!is_safe_split(0, 4, 2, 4, 6));
    }

    #[test]
    fn test_same_start_same_len() {
        // Exact same region - definitely overlapping
        assert!(!is_safe_split(0, 3, 0, 3, 6));
    }

    #[test]
    fn test_out_of_bounds() {
        // Second region extends past end
        assert!(!is_safe_split(0, 3, 4, 4, 6));
    }

    #[test]
    fn test_zero_length_same_start() {
        // Two zero-length slices at the same position
        // This is actually safe - no memory is accessed
        assert!(is_safe_split(3, 0, 3, 0, 6));
    }

    #[test]
    fn test_zero_length_no_overlap() {
        // Zero-length slice at position where non-zero slice exists
        // [0,1,2] and [] at position 2 - safe because [] accesses nothing
        assert!(is_safe_split(0, 3, 2, 0, 6));
    }

    #[test]
    fn test_adjacent_regions() {
        // [0,1] and [2,3] are adjacent but don't overlap
        assert!(is_safe_split(0, 2, 2, 2, 4));
    }

    #[test]
    fn test_reversed_order() {
        // Second region comes before first - should still work
        assert!(is_safe_split(3, 3, 0, 3, 6));
    }

    #[test]
    fn test_one_inside_other() {
        // [0,1,2,3,4] contains [1,2,3]
        assert!(!is_safe_split(0, 5, 1, 3, 6));
    }

    #[test]
    fn test_overflow_protection() {
        // Pathological case: start + len would overflow
        assert!(!is_safe_split(usize::MAX, 1, 0, 1, 10));
    }
}

// ============================================================================
// Task 10: The Dangling Closure
// ============================================================================
//
// You're building a callback registry for a plugin system.
// Callbacks are stored as (context_pointer, function_pointer) pairs,
// similar to how C callbacks work with void* userdata.
//
// A user reports crashes. Here's the pattern that breaks:
//
// ```
// fn broken_example(registry: &mut CallbackRegistry) {
//     let local_data = String::from("important");
//     let ctx_ptr = &local_data as *const String as *mut std::ffi::c_void;
//     registry.register_raw(ctx_ptr, my_callback);
// } // local_data dropped here, ctx_ptr is now dangling!
//
// fn later() {
//     registry.fire_all(); // CRASH: callbacks use dangling pointers
// }
// ```
//
// Your task: implement a safe registration API that takes ownership.

use std::ffi::c_void;

pub type CallbackFn = fn(*mut c_void);

pub struct CallbackRegistry {
    callbacks: Vec<(*mut c_void, CallbackFn)>,
}

impl CallbackRegistry {
    pub fn new() -> Self {
        Self {
            callbacks: Vec::new(),
        }
    }

    /// UNSAFE API: Registers a raw pointer. Caller responsible for lifetime.
    /// This is what the buggy code uses.
    pub fn register_raw(&mut self, ctx: *mut c_void, f: CallbackFn) {
        self.callbacks.push((ctx, f));
    }

    /// SAFE API: Takes ownership of the context data.
    /// The registry now owns the data and will keep it alive.
    /// Returns an ID that can be used to unregister.
    ///
    /// Hint: You need to put `ctx` somewhere that outlives the registration.
    pub fn register_owned<T: 'static>(&mut self, ctx: T, f: CallbackFn) -> usize {
        todo!()
    }

    /// Unregister and return ownership of the context data.
    /// Returns None if ID is invalid or type doesn't match.
    ///
    /// The tricky part: you stored a *mut c_void, but you need to
    /// return a Box<T>. How do you safely recover the type?
    pub fn unregister<T: 'static>(&mut self, id: usize) -> Option<Box<T>> {
        todo!()
    }

    /// Fire all callbacks. Each callback receives its context pointer.
    pub fn fire_all(&self) {
        for (ctx, f) in &self.callbacks {
            f(*ctx);
        }
    }
}

impl Default for CallbackRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod task10 {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static CALL_COUNT: AtomicUsize = AtomicUsize::new(0);
    static LAST_VALUE: AtomicUsize = AtomicUsize::new(0);

    fn test_callback(ctx: *mut c_void) {
        CALL_COUNT.fetch_add(1, Ordering::SeqCst);
        let value = unsafe { *(ctx as *const usize) };
        LAST_VALUE.store(value, Ordering::SeqCst);
    }

    #[test]
    fn test_register_and_fire() {
        CALL_COUNT.store(0, Ordering::SeqCst);
        LAST_VALUE.store(0, Ordering::SeqCst);

        let mut registry = CallbackRegistry::new();
        let data: usize = 42;
        let _id = registry.register_owned(data, test_callback);

        registry.fire_all();

        assert_eq!(CALL_COUNT.load(Ordering::SeqCst), 1);
        assert_eq!(LAST_VALUE.load(Ordering::SeqCst), 42);
    }

    #[test]
    fn test_unregister_returns_data() {
        let mut registry = CallbackRegistry::new();
        let data = Box::new(String::from("hello"));
        let id = registry.register_owned(data, |_| {});

        let recovered: Option<Box<Box<String>>> = registry.unregister(id);
        assert!(recovered.is_some());
        assert_eq!(**recovered.unwrap(), "hello");
    }

    #[test]
    fn test_data_lives_until_unregister() {
        // This test verifies the data isn't dropped prematurely
        let mut registry = CallbackRegistry::new();

        // Register in a scope
        let id = {
            let data = vec![1, 2, 3, 4, 5];
            registry.register_owned(data, |_| {})
        };
        // data would be dropped here if register_owned didn't take ownership

        // But we can still unregister and get it back
        let recovered: Option<Box<Vec<i32>>> = registry.unregister(id);
        assert!(recovered.is_some());
        assert_eq!(*recovered.unwrap(), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_invalid_id() {
        let mut registry = CallbackRegistry::new();
        let recovered: Option<Box<usize>> = registry.unregister(999);
        assert!(recovered.is_none());
    }
}

// ============================================================================
// Task 11: Alignment Roulette
// ============================================================================
//
// You're implementing a simple bump allocator that carves allocations
// out of a byte buffer. It works great on x86, but crashes on ARM.
//
// The problem: x86 tolerates misaligned access (slowly), ARM doesn't.
// A u32 must be at an address divisible by 4. A u64 by 8. Etc.
//
// Your task: implement an allocator that respects alignment.

/// Check if a pointer is properly aligned for type T.
pub fn is_aligned<T>(ptr: *const u8) -> bool {
    todo!()
}

/// Allocate space for a T in the buffer, returning a properly aligned pointer.
///
/// - `buffer`: the byte buffer to allocate from
/// - `offset`: current position in buffer (updated on success)
///
/// Returns None if there isn't enough space (accounting for alignment padding).
///
/// The returned pointer must be aligned for T. This may require skipping
/// some bytes in the buffer.
pub fn alloc_in_buffer<T>(buffer: &mut [u8], offset: &mut usize) -> Option<*mut T> {
    todo!()
}

/// Write a value into the buffer at the given offset.
/// Returns the new offset after the write, or None if it doesn't fit.
///
/// Must handle alignment correctly.
pub fn write_to_buffer<T>(buffer: &mut [u8], offset: &mut usize, value: T) -> Option<()> {
    todo!()
}

/// Read a value from the buffer at the given offset.
/// Returns None if the read would be out of bounds or misaligned.
pub fn read_from_buffer<T: Copy>(buffer: &[u8], offset: usize) -> Option<T> {
    todo!()
}

#[cfg(test)]
mod task11 {
    use super::*;

    #[test]
    fn test_is_aligned() {
        // u32 requires 4-byte alignment
        assert!(is_aligned::<u32>((0x1000 as *const u8)));
        assert!(is_aligned::<u32>((0x1004 as *const u8)));
        assert!(!is_aligned::<u32>((0x1001 as *const u8)));
        assert!(!is_aligned::<u32>((0x1002 as *const u8)));

        // u8 requires 1-byte alignment (always aligned)
        assert!(is_aligned::<u8>((0x1001 as *const u8)));

        // u64 requires 8-byte alignment
        assert!(is_aligned::<u64>((0x1000 as *const u8)));
        assert!(!is_aligned::<u64>((0x1004 as *const u8)));
    }

    #[test]
    fn test_alloc_simple() {
        let mut buffer = [0u8; 64];
        let mut offset = 0;

        let ptr = alloc_in_buffer::<u32>(&mut buffer, &mut offset);
        assert!(ptr.is_some());
        assert!(is_aligned::<u32>(ptr.unwrap() as *const u8));
    }

    #[test]
    fn test_alloc_respects_alignment() {
        let mut buffer = [0u8; 64];
        let mut offset = 1; // Start misaligned

        let ptr = alloc_in_buffer::<u32>(&mut buffer, &mut offset);
        assert!(ptr.is_some());
        // Should have skipped to offset 4
        assert!(is_aligned::<u32>(ptr.unwrap() as *const u8));
        assert!(offset >= 4 + std::mem::size_of::<u32>());
    }

    #[test]
    fn test_alloc_out_of_space() {
        let mut buffer = [0u8; 8];
        let mut offset = 0;

        // First allocation should succeed
        let ptr1 = alloc_in_buffer::<u64>(&mut buffer, &mut offset);
        assert!(ptr1.is_some());

        // Second should fail - no space
        let ptr2 = alloc_in_buffer::<u64>(&mut buffer, &mut offset);
        assert!(ptr2.is_none());
    }

    #[test]
    fn test_write_and_read() {
        let mut buffer = [0u8; 32];
        let mut offset = 0;

        // Write a u32
        write_to_buffer(&mut buffer, &mut offset, 0xDEADBEEFu32).unwrap();

        // Write a u64 (will need alignment padding)
        write_to_buffer(&mut buffer, &mut offset, 0x123456789ABCDEFu64).unwrap();

        // Read them back
        let v1: u32 = read_from_buffer(&buffer, 0).unwrap();
        assert_eq!(v1, 0xDEADBEEF);

        // u64 should be at offset 8 (aligned)
        let v2: u64 = read_from_buffer(&buffer, 8).unwrap();
        assert_eq!(v2, 0x123456789ABCDEF);
    }

    #[test]
    fn test_read_misaligned_fails() {
        let buffer = [0u8; 32];
        // Try to read u32 from odd offset
        let result: Option<u32> = read_from_buffer(&buffer, 1);
        assert!(result.is_none());
    }
}

// ============================================================================
// Task 12: The Slice Reassembly
// ============================================================================
//
// You're implementing a zero-copy parser. Data arrives in chunks, and
// sometimes multiple chunks are actually contiguous in memory (e.g.,
// from a memory-mapped file read in pieces).
//
// When chunks are contiguous, you want to return a single slice spanning
// all of them - avoiding copies. When they're not, you need to copy.
//
// The tricky part: the lifetime of the returned slice.

/// A chunk of data: pointer to start and length.
pub struct Chunk<T> {
    pub ptr: *const T,
    pub len: usize,
}

/// If all chunks are contiguous in memory (each chunk starts exactly
/// where the previous one ends), return a single slice spanning them all.
///
/// Returns None if:
/// - chunks is empty
/// - any chunk has a null pointer
/// - chunks are not contiguous
///
/// IMPORTANT: Think about the lifetime annotation. The returned slice
/// must not outlive the data the chunks point to. But the chunks themselves
/// are just metadata (pointers) - they don't own the data.
pub unsafe fn try_merge_contiguous<'a, T>(chunks: &[Chunk<T>]) -> Option<&'a [T]> {
    todo!()
}

/// Copy data from multiple chunks into a destination buffer.
/// Returns the total number of elements copied, or an error.
#[derive(Debug, PartialEq)]
pub enum CoalesceError {
    BufferTooSmall { needed: usize, available: usize },
    NullPointer { chunk_index: usize },
}

pub unsafe fn coalesce_chunks<T: Copy>(
    chunks: &[Chunk<T>],
    dest: *mut T,
    dest_capacity: usize,
) -> Result<usize, CoalesceError> {
    todo!()
}

#[cfg(test)]
mod task12 {
    use super::*;

    #[test]
    fn test_contiguous_merge() {
        let data = [1, 2, 3, 4, 5, 6];
        let chunks = [
            Chunk {
                ptr: data[0..2].as_ptr(),
                len: 2,
            },
            Chunk {
                ptr: data[2..4].as_ptr(),
                len: 2,
            },
            Chunk {
                ptr: data[4..6].as_ptr(),
                len: 2,
            },
        ];

        let merged = unsafe { try_merge_contiguous(&chunks) };
        assert!(merged.is_some());
        assert_eq!(merged.unwrap(), &[1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_non_contiguous_fails() {
        let data1 = [1, 2, 3];
        let data2 = [4, 5, 6]; // Different allocation

        let chunks = [
            Chunk {
                ptr: data1.as_ptr(),
                len: 3,
            },
            Chunk {
                ptr: data2.as_ptr(),
                len: 3,
            },
        ];

        let merged = unsafe { try_merge_contiguous(&chunks) };
        assert!(merged.is_none());
    }

    #[test]
    fn test_empty_chunks() {
        let chunks: &[Chunk<i32>] = &[];
        let merged = unsafe { try_merge_contiguous(chunks) };
        assert!(merged.is_none());
    }

    #[test]
    fn test_single_chunk() {
        let data = [1, 2, 3];
        let chunks = [Chunk {
            ptr: data.as_ptr(),
            len: 3,
        }];

        let merged = unsafe { try_merge_contiguous(&chunks) };
        assert!(merged.is_some());
        assert_eq!(merged.unwrap(), &[1, 2, 3]);
    }

    #[test]
    fn test_null_pointer() {
        let chunks = [Chunk::<i32> {
            ptr: std::ptr::null(),
            len: 3,
        }];
        let merged = unsafe { try_merge_contiguous(&chunks) };
        assert!(merged.is_none());
    }

    #[test]
    fn test_coalesce_success() {
        let data1 = [1, 2];
        let data2 = [3, 4];
        let chunks = [
            Chunk {
                ptr: data1.as_ptr(),
                len: 2,
            },
            Chunk {
                ptr: data2.as_ptr(),
                len: 2,
            },
        ];

        let mut dest = [0i32; 4];
        let count = unsafe { coalesce_chunks(&chunks, dest.as_mut_ptr(), 4) };

        assert_eq!(count, Ok(4));
        assert_eq!(dest, [1, 2, 3, 4]);
    }

    #[test]
    fn test_coalesce_buffer_too_small() {
        let data = [1, 2, 3, 4, 5];
        let chunks = [Chunk {
            ptr: data.as_ptr(),
            len: 5,
        }];

        let mut dest = [0i32; 3];
        let result = unsafe { coalesce_chunks(&chunks, dest.as_mut_ptr(), 3) };

        assert_eq!(
            result,
            Err(CoalesceError::BufferTooSmall {
                needed: 5,
                available: 3
            })
        );
    }
}

// ============================================================================
// Task 13: Type Punning Minefield
// ============================================================================
//
// You're porting C code that reinterprets memory. Some casts are safe,
// others are UB in Rust (even if they worked in C).
//
// Rules to internalize:
// - Size must match (or you're reading garbage / writing past bounds)
// - Alignment must be compatible (T's alignment must be >= U's, or you misalign)
// - Rust makes fewer layout guarantees than C
//
// Your task: implement safe transmutation helpers.

/// Attempt to reinterpret a pointer to T as a pointer to U.
/// Returns None if the cast would be unsound.
///
/// A cast is sound if:
/// - size_of::<T>() == size_of::<U>()
/// - align_of::<T>() >= align_of::<U>() (the pointer is at least as aligned)
/// - The pointer is non-null
///
/// NOTE: This only checks pointer compatibility, not data validity.
/// Casting *const u32 to *const f32 may give you a garbage float.
pub fn try_cast_ptr<T, U>(ptr: *const T) -> Option<*const U> {
    todo!()
}

/// Read a native-endian u32 from a byte slice without undefined behavior.
///
/// Returns None if:
/// - offset + 4 > bytes.len() (out of bounds)
/// - The read would be misaligned (you'll need to handle this!)
///
/// Hint: You cannot just cast &bytes[offset] to *const u32 and dereference.
/// That's UB if the address isn't 4-byte aligned. There's a safe way to
/// read potentially misaligned data.
pub fn read_ne_u32(bytes: &[u8], offset: usize) -> Option<u32> {
    todo!()
}

/// Read a native-endian u16 from a byte slice without undefined behavior.
pub fn read_ne_u16(bytes: &[u8], offset: usize) -> Option<u16> {
    todo!()
}

#[cfg(test)]
mod task13 {
    use super::*;

    #[test]
    fn test_same_size_cast() {
        let x: u32 = 42;
        let ptr = &x as *const u32;

        // u32 to i32: same size and alignment
        let cast = try_cast_ptr::<u32, i32>(ptr);
        assert!(cast.is_some());
    }

    #[test]
    fn test_different_size_fails() {
        let x: u32 = 42;
        let ptr = &x as *const u32;

        // u32 to u64: different sizes
        let cast = try_cast_ptr::<u32, u64>(ptr);
        assert!(cast.is_none());
    }

    #[test]
    fn test_alignment_downgrade_ok() {
        let x: u64 = 42;
        let ptr = &x as *const u64;

        // u64 (align 8) to [u8; 8] (align 1): OK because 8 >= 1
        let cast = try_cast_ptr::<u64, [u8; 8]>(ptr);
        assert!(cast.is_some());
    }

    #[test]
    fn test_null_fails() {
        let ptr: *const u32 = std::ptr::null();
        let cast = try_cast_ptr::<u32, i32>(ptr);
        assert!(cast.is_none());
    }

    #[test]
    fn test_read_ne_u32_aligned() {
        let bytes: [u8; 8] = [0xEF, 0xBE, 0xAD, 0xDE, 0, 0, 0, 0];
        // On little-endian, this should be 0xDEADBEEF
        let value = read_ne_u32(&bytes, 0);
        assert!(value.is_some());

        #[cfg(target_endian = "little")]
        assert_eq!(value.unwrap(), 0xDEADBEEF);
    }

    #[test]
    fn test_read_ne_u32_misaligned() {
        // Create a buffer where offset 1 is misaligned
        let bytes: [u8; 8] = [0, 0xEF, 0xBE, 0xAD, 0xDE, 0, 0, 0];

        // Reading from offset 1 should still work (no UB)
        // because we handle misalignment properly
        let value = read_ne_u32(&bytes, 1);
        assert!(value.is_some());

        #[cfg(target_endian = "little")]
        assert_eq!(value.unwrap(), 0xDEADBEEF);
    }

    #[test]
    fn test_read_ne_u32_out_of_bounds() {
        let bytes: [u8; 4] = [1, 2, 3, 4];
        // Offset 2 would read bytes 2,3,4,5 but 5 doesn't exist
        let value = read_ne_u32(&bytes, 2);
        assert!(value.is_none());
    }

    #[test]
    fn test_read_ne_u16() {
        let bytes: [u8; 4] = [0x34, 0x12, 0, 0];
        let value = read_ne_u16(&bytes, 0);
        assert!(value.is_some());

        #[cfg(target_endian = "little")]
        assert_eq!(value.unwrap(), 0x1234);
    }
}

// ============================================================================
// Task 14: Iterator Invalidation
// ============================================================================
//
// Classic C++ footgun: iterating over a vector while modifying it.
// The vector reallocates, your iterator now points to freed memory.
//
// In safe Rust, the borrow checker prevents this. But with raw pointers,
// you're on your own.
//
// Your task: implement a Vec-like container with invalidation detection.
// Use a generation counter that increments on each reallocation.
// Iterators store the generation at creation time and check it on each access.

#[derive(Debug, PartialEq)]
pub struct IteratorInvalidated;

pub struct UnsafeVec<T> {
    ptr: *mut T,
    len: usize,
    capacity: usize,
    generation: usize,
}

pub struct UnsafeVecIter<'a, T> {
    vec: &'a UnsafeVec<T>,
    index: usize,
    generation: usize,
}

impl<T> UnsafeVec<T> {
    /// Create a new empty UnsafeVec.
    pub fn new() -> Self {
        todo!()
    }

    /// Push a value. May reallocate (which increments generation).
    pub fn push(&mut self, value: T) {
        todo!()
    }

    /// Get a raw pointer to an element. Returns None if out of bounds.
    pub fn get_ptr(&self, index: usize) -> Option<*const T> {
        todo!()
    }

    /// Get the current length.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get the current generation (for testing).
    pub fn generation(&self) -> usize {
        self.generation
    }

    /// Create an iterator. The iterator captures the current generation.
    pub fn iter(&self) -> UnsafeVecIter<'_, T> {
        UnsafeVecIter {
            vec: self,
            index: 0,
            generation: self.generation,
        }
    }
}

impl<T> Default for UnsafeVec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for UnsafeVec<T> {
    fn drop(&mut self) {
        todo!()
    }
}

impl<'a, T> UnsafeVecIter<'a, T> {
    /// Get the next element, or an error if the vector was reallocated.
    ///
    /// Returns:
    /// - Ok(Some(&T)) if there's a next element
    /// - Ok(None) if iteration is complete
    /// - Err(IteratorInvalidated) if the vector reallocated since iter creation
    pub fn next_checked(&mut self) -> Result<Option<&'a T>, IteratorInvalidated> {
        todo!()
    }
}

#[cfg(test)]
mod task14 {
    use super::*;

    #[test]
    fn test_basic_push_and_get() {
        let mut v = UnsafeVec::new();
        v.push(1);
        v.push(2);
        v.push(3);

        assert_eq!(v.len(), 3);
        assert_eq!(unsafe { *v.get_ptr(0).unwrap() }, 1);
        assert_eq!(unsafe { *v.get_ptr(1).unwrap() }, 2);
        assert_eq!(unsafe { *v.get_ptr(2).unwrap() }, 3);
    }

    #[test]
    fn test_out_of_bounds() {
        let mut v = UnsafeVec::new();
        v.push(1);
        assert!(v.get_ptr(5).is_none());
    }

    #[test]
    fn test_iter_success() {
        let mut v = UnsafeVec::new();
        v.push(10);
        v.push(20);
        v.push(30);

        let mut iter = v.iter();
        assert_eq!(iter.next_checked(), Ok(Some(&10)));
        assert_eq!(iter.next_checked(), Ok(Some(&20)));
        assert_eq!(iter.next_checked(), Ok(Some(&30)));
        assert_eq!(iter.next_checked(), Ok(None));
    }

    #[test]
    fn test_iter_invalidation() {
        let mut v = UnsafeVec::new();
        // Push enough to trigger reallocation
        for i in 0..4 {
            v.push(i);
        }

        let initial_gen = v.generation();
        let mut iter = v.iter();

        // Read one element
        assert_eq!(iter.next_checked(), Ok(Some(&0)));

        // Force reallocation by pushing more
        for i in 4..100 {
            v.push(i);
        }

        // Generation should have changed
        assert!(v.generation() > initial_gen);

        // Iterator should detect invalidation
        assert_eq!(iter.next_checked(), Err(IteratorInvalidated));
    }

    #[test]
    fn test_generation_stable_without_realloc() {
        let mut v: UnsafeVec<i32> = UnsafeVec::new();
        // Pre-allocate by pushing and popping (or just track generation)
        let gen1 = v.generation();

        // Pushes that don't exceed capacity shouldn't change generation
        // (This test depends on your growth strategy - adjust as needed)
        v.push(1);
        v.push(2);

        // If capacity was 0 initially, first push will reallocate
        // After that, pushes within capacity shouldn't change generation
        let gen2 = v.generation();
        let cap = v.capacity; // We'd need a getter, or just test the concept

        // The key insight: generation only changes on reallocation
    }

    #[test]
    fn test_drop_frees_memory() {
        // This test mainly verifies no memory leak / double-free
        // Run with valgrind or miri to verify
        let mut v = UnsafeVec::new();
        v.push(String::from("hello"));
        v.push(String::from("world"));
        // v goes out of scope and should clean up properly
    }
}

// ============================================================================
// Task 15: The Function Pointer Thunk
// ============================================================================
//
// You're building a plugin system. Plugins expose extern "C" functions.
// You want to wrap these functions to add logging.
//
// The challenge: function pointers cannot capture state.
// A closure that captures variables cannot be converted to fn().
//
// Your task:
// 1. Explain (in a doc comment) why the naive approach is impossible
// 2. Implement a workaround using a struct wrapper

/// A C-compatible function pointer type.
pub type PluginFn = extern "C" fn(i32) -> i32;

/// A log entry for function calls.
#[derive(Debug, Clone, PartialEq)]
pub struct CallLog {
    pub name: &'static str,
    pub input: i32,
    pub output: i32,
}

/// IMPOSSIBLE FUNCTION - Explain why in the doc comment, then leave as todo!()
///
/// We want to return a PluginFn that logs calls to `f` with the given `name`.
/// But this is impossible. Why?
///
/// Your explanation should cover:
/// - What a function pointer is (vs a closure)
/// - Why capturing `name` and `log` is necessary for logging
/// - Why function pointers can't capture anything
/// - What the fundamental limitation is
///
/// Write your explanation here (replace this text):
/// TODO: Explain why this is impossible
pub fn wrap_with_logging(_f: PluginFn, _name: &'static str, _log: &mut Vec<CallLog>) -> PluginFn {
    // This function cannot be implemented correctly.
    // The return type PluginFn is `extern "C" fn(i32) -> i32` - a raw function pointer.
    // It has no way to "remember" f, name, or log.
    todo!("Impossible to implement - see doc comment for explanation")
}

/// A wrapper that CAN log function calls because it's a struct (has state).
pub struct PluginWrapper {
    f: PluginFn,
    name: &'static str,
    log: Vec<CallLog>,
}

impl PluginWrapper {
    /// Create a new wrapper.
    pub fn new(f: PluginFn, name: &'static str) -> Self {
        todo!()
    }

    /// Call the wrapped function, logging the call.
    pub fn call(&mut self, arg: i32) -> i32 {
        todo!()
    }

    /// Get the call log.
    pub fn log(&self) -> &[CallLog] {
        todo!()
    }

    /// Clear the log.
    pub fn clear_log(&mut self) {
        todo!()
    }
}

#[cfg(test)]
mod task15 {
    use super::*;

    extern "C" fn double(x: i32) -> i32 {
        x * 2
    }

    extern "C" fn add_ten(x: i32) -> i32 {
        x + 10
    }

    #[test]
    fn test_wrapper_basic() {
        let mut wrapper = PluginWrapper::new(double, "double");
        let result = wrapper.call(5);
        assert_eq!(result, 10);
    }

    #[test]
    fn test_wrapper_logs_calls() {
        let mut wrapper = PluginWrapper::new(double, "double");
        wrapper.call(5);
        wrapper.call(7);

        let log = wrapper.log();
        assert_eq!(log.len(), 2);
        assert_eq!(
            log[0],
            CallLog {
                name: "double",
                input: 5,
                output: 10
            }
        );
        assert_eq!(
            log[1],
            CallLog {
                name: "double",
                input: 7,
                output: 14
            }
        );
    }

    #[test]
    fn test_wrapper_clear_log() {
        let mut wrapper = PluginWrapper::new(add_ten, "add_ten");
        wrapper.call(1);
        wrapper.call(2);
        assert_eq!(wrapper.log().len(), 2);

        wrapper.clear_log();
        assert_eq!(wrapper.log().len(), 0);

        wrapper.call(3);
        assert_eq!(wrapper.log().len(), 1);
        assert_eq!(wrapper.log()[0].output, 13);
    }

    #[test]
    fn test_different_functions() {
        let mut w1 = PluginWrapper::new(double, "double");
        let mut w2 = PluginWrapper::new(add_ten, "add_ten");

        assert_eq!(w1.call(5), 10);
        assert_eq!(w2.call(5), 15);

        assert_eq!(w1.log()[0].name, "double");
        assert_eq!(w2.log()[0].name, "add_ten");
    }
}
