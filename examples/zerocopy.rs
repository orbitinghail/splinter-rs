use splinter_rs::{
    CowSplinter, Encodable, PartitionRead, PartitionWrite, Splinter, SplinterRef, testutil::SetGen,
};

fn main() {
    let mut setgen = SetGen::new(0xDEAD_BEEF);

    // create a splinter filled with random u32 values
    let splinter = Splinter::from_iter(setgen.random_max(4096, 16384));

    // retrieve a value contained by the splinter for later
    let value = splinter.select(9).unwrap();

    // serialize the splinter to a byte buffer
    let bytes = splinter.encode_to_bytes();

    println!("Serialized splinter size: {} bytes", bytes.len());
    println!("First 32 bytes of serialized splinter:");
    // print out the first 32 bytes of the serialized splinter in hex
    for byte in bytes.iter().take(32) {
        print!("{:02X} ", byte);
    }
    println!();

    // open the serialized splinter as a SplinterRef (zero-copy)
    // this function accepts any type which implements Deref<Target=[u8]>
    // which means it works with most byte buffer types in Rust
    let splinter_ref = SplinterRef::from_bytes(bytes).unwrap();

    // check that the two splinters are equivalent
    assert_eq!(splinter, splinter_ref);

    // or just run bitmap operations directly on the SplinterRef
    assert!(splinter_ref.contains(value));

    // for convenience, you can create a CowSplinter to enable copy-on-write
    let mut splinter_cow = CowSplinter::from(splinter_ref);

    // copy-on-write means we can run mutable operations on a splinter ref, causing it to be fully deserialized
    splinter_cow.insert(123456);
    assert!(splinter_cow.contains(123456));

    println!("Success!");
}
