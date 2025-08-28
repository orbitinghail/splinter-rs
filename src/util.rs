use std::iter::Peekable;

#[doc(hidden)]
#[macro_export]
macro_rules! MultiIter {
    ($type:ident, $($name:ident),+) => {
        pub(crate) enum $type<$($name),+> {
            $($name($name)),+
        }

        impl<
            T, $($name: Iterator<Item=T>),+
        > Iterator for $type<$($name),+>
        {
            type Item = T;

            fn next(&mut self) -> Option<Self::Item> {
                match self {
                    $(Self::$name(iter) => iter.next(),)+
                }
            }
        }
    };
}

pub fn find_next_sorted<I, T>(iter: &mut Peekable<I>, needle: &T) -> Option<T>
where
    I: Iterator<Item = T>,
    T: PartialOrd + PartialEq,
{
    // advance the iterator until either:
    // 1. we find the needle
    // 2. we find a value larger than the needle
    //
    while let Some(next) = iter.next_if(|v| v <= needle) {
        if &next == needle {
            return Some(next);
        }
    }
    None
}
