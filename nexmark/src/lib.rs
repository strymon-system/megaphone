extern crate abomonation;
#[macro_use]
extern crate abomonation_derive;
extern crate timely;
extern crate timely_communication;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate rand;

pub mod config;
pub mod event;

// currently enables UB, by exposing padding bytes
#[inline] unsafe fn typed_to_bytes<T>(slice: &[T]) -> &[u8] {
    std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * ::std::mem::size_of::<T>())
}

#[derive(Clone)]
pub struct AbomVecDeque<T: ::abomonation::Abomonation>(pub ::std::collections::VecDeque<T>);

impl<T: ::abomonation::Abomonation> ::std::ops::Deref for AbomVecDeque<T> {
    type Target = ::std::collections::VecDeque<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: ::abomonation::Abomonation> ::std::ops::DerefMut for AbomVecDeque<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: ::abomonation::Abomonation> ::abomonation::Abomonation for AbomVecDeque<T> {
    #[inline]
    unsafe fn entomb<W: ::std::io::Write>(&self, write: &mut W) -> ::std::io::Result<()> {
        let (first_slice, second_slice) = self.as_slices();
        write.write_all(typed_to_bytes(first_slice))?;
        write.write_all(typed_to_bytes(second_slice))?;
        for element in self.iter() { element.entomb(write)?; }
        Ok(())
    }
    #[inline]
    unsafe fn exhume<'a,'b>(&'a mut self, bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {

        // extract memory from bytes to back our vector
        let binary_len = self.len() * ::std::mem::size_of::<T>();
        if binary_len > bytes.len() { None }
        else {
            let (mine, mut rest) = bytes.split_at_mut(binary_len);
            let slice = std::slice::from_raw_parts_mut(mine.as_mut_ptr() as *mut T, self.len());
            let vec = Vec::from_raw_parts(slice.as_mut_ptr(), self.len(), self.len());
            std::ptr::write(self, AbomVecDeque(::std::collections::VecDeque::from(vec)));
            for element in self.iter_mut() {
                let temp = rest;             // temp variable explains lifetimes (mysterious!)
                rest = element.exhume(temp)?;
            }
            Some(rest)
        }
    }
    #[inline] 
    fn extent(&self) -> usize {
        let mut sum = ::std::mem::size_of::<T>() * self.len();
        for element in self.iter() {
            sum += element.extent();
        }
        sum
    }
}
