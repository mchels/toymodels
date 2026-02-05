//! Rust Pointer Exercises
//!
//! Run tests with: cargo test
//! Run specific task: cargo test task1

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
    todo!()
}

/// Convert address to a const raw pointer to u32
pub fn addr_to_ptr_const(addr: usize) -> *const u32 {
    todo!()
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
    todo!()
}

/// Convert a mutable reference to a mutable raw pointer
pub fn ref_to_mut_ptr<T>(r: &mut T) -> *mut T {
    todo!()
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
    todo!()
}

/// Write a value through a mutable raw pointer.
///
/// # Safety
/// ptr must point to valid, writable memory for T
pub unsafe fn write_through_ptr<T>(ptr: *mut T, value: T) {
    todo!()
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
    todo!()
}

/// Convert a mutable raw pointer to a mutable reference.
///
/// # Safety
/// ptr must be valid, aligned, point to initialized data,
/// and no other references to the data may exist.
pub unsafe fn ptr_to_mut_ref<'a, T>(ptr: *mut T) -> &'a mut T {
    todo!()
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
    todo!()
}

/// Set the y field of a Point through a raw pointer.
///
/// # Safety
/// ptr must point to a valid, writable Point
pub unsafe fn write_y(ptr: *mut Point, value: i32) {
    todo!()
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
    todo!()
}

/// Get the raw pointer from a NonNull.
pub fn unwrap_nonnull<T>(nn: NonNull<T>) -> *mut T {
    todo!()
}

/// Read a value through NonNull.
///
/// # Safety
/// The NonNull must point to valid, initialized data.
pub unsafe fn read_via_nonnull<T: Copy>(nn: NonNull<T>) -> T {
    todo!()
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
    todo!()
}

/// Convert a raw pointer back to a Box (takes ownership).
///
/// # Safety
/// ptr must have come from Box::into_raw and not been freed.
pub unsafe fn raw_to_box<T>(ptr: *mut T) -> Box<T> {
    todo!()
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
    todo!()
}

/// Get a mutable reference to the element in a node.
///
/// # Safety
/// nn must point to a valid Node, and no other references may exist.
pub unsafe fn get_elem_mut<'a, T>(nn: NonNull<Node<T>>) -> &'a mut T {
    todo!()
}

/// Set the next pointer of a node.
///
/// # Safety
/// nn must point to a valid Node
pub unsafe fn set_next<T>(nn: NonNull<Node<T>>, next: Option<NonNull<Node<T>>>) {
    todo!()
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
