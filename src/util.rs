use anyhow::Result;

pub fn read_varint(input: &[u8]) -> Result<(u64, &[u8])> {
    let mut bytes = input.iter();
    let mut varint: u64 = 0;
    let mut msb = 1;

    let mut bytes_consumed = 0;
    while msb == 1 {
        let byte = bytes.next().unwrap();
        varint = varint << 7;
        varint += (byte & 0x7F) as u64;
        msb = byte >> 7;
        bytes_consumed += 1;
    }
    Ok((varint, &input[bytes_consumed..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_from_bytes() {
        let x: [u8; 3] = [0xAC, 0x02, 0xAA]; // last byte should be ignored
        let varint = read_varint(&x).unwrap().0;

        assert_eq!(varint, 5634);
    }
}
