use super::store::SMT_KEY;
use sparse_merkle_tree::{merge::MergeValue, BranchKey, BranchNode};
use std::convert::TryInto;

/// Serialize a `BranchKey` into a `Vec<u8>` for use as a key in the key-value store.
pub fn branch_key_to_vec(key: &BranchKey) -> Vec<u8> {
    let mut ret = Vec::with_capacity(34);
    ret.extend_from_slice(SMT_KEY);
    ret.extend_from_slice(&[key.height]);
    ret.extend_from_slice(key.node_key.as_slice());
    ret
}

/// Serialize a `BranchNode` into a `Vec<u8>` for use as a key in the key-value store.
pub fn branch_node_to_vec(node: &BranchNode) -> Vec<u8> {
    match (&node.left, &node.right) {
        (MergeValue::Value(left), MergeValue::Value(right)) => {
            let mut ret = Vec::with_capacity(34);
            ret.extend_from_slice(SMT_KEY);
            ret.extend_from_slice(&[0]);
            ret.extend_from_slice(left.as_slice());
            ret.extend_from_slice(right.as_slice());
            ret
        }
        (
            MergeValue::Value(left),
            MergeValue::MergeWithZero {
                base_node,
                zero_bits,
                zero_count,
            },
        ) => {
            let mut ret = Vec::with_capacity(99);
            ret.extend_from_slice(SMT_KEY);
            ret.extend_from_slice(&[1]);
            ret.extend_from_slice(left.as_slice());
            ret.extend_from_slice(base_node.as_slice());
            ret.extend_from_slice(zero_bits.as_slice());
            ret.extend_from_slice(&[*zero_count]);
            ret
        }
        (
            MergeValue::MergeWithZero {
                base_node,
                zero_bits,
                zero_count,
            },
            MergeValue::Value(right),
        ) => {
            let mut ret = Vec::with_capacity(99);
            ret.extend_from_slice(SMT_KEY);
            ret.extend_from_slice(&[2]);
            ret.extend_from_slice(base_node.as_slice());
            ret.extend_from_slice(zero_bits.as_slice());
            ret.extend_from_slice(&[*zero_count]);
            ret.extend_from_slice(right.as_slice());
            ret
        }
        (
            MergeValue::MergeWithZero {
                base_node: l_base_node,
                zero_bits: l_zero_bits,
                zero_count: l_zero_count,
            },
            MergeValue::MergeWithZero {
                base_node: r_base_node,
                zero_bits: r_zero_bits,
                zero_count: r_zero_count,
            },
        ) => {
            let mut ret = Vec::with_capacity(132);
            ret.extend_from_slice(SMT_KEY);
            ret.extend_from_slice(&[3]);
            ret.extend_from_slice(l_base_node.as_slice());
            ret.extend_from_slice(l_zero_bits.as_slice());
            ret.extend_from_slice(&[*l_zero_count]);
            ret.extend_from_slice(r_base_node.as_slice());
            ret.extend_from_slice(r_zero_bits.as_slice());
            ret.extend_from_slice(&[*r_zero_count]);
            ret
        }
        (MergeValue::Value(left), MergeValue::ShortCut { key, value, height }) => {
            let mut ret = Vec::with_capacity(99);
            ret.extend_from_slice(SMT_KEY);
            ret.extend_from_slice(&[4]);
            ret.extend_from_slice(left.as_slice());
            ret.extend_from_slice(key.as_slice());
            ret.extend_from_slice(value.as_slice());
            ret.extend_from_slice(&[*height]);
            ret
        }
        (MergeValue::ShortCut { key, value, height }, MergeValue::Value(right)) => {
            let mut ret = Vec::with_capacity(99);
            ret.extend_from_slice(SMT_KEY);
            ret.extend_from_slice(&[5]);
            ret.extend_from_slice(key.as_slice());
            ret.extend_from_slice(value.as_slice());
            ret.extend_from_slice(&[*height]);
            ret.extend_from_slice(right.as_slice());
            ret
        }
        (
            MergeValue::ShortCut {
                key: l_key,
                value: l_value,
                height: l_height,
            },
            MergeValue::ShortCut {
                key: r_key,
                value: r_value,
                height: r_height,
            },
        ) => {
            let mut ret = Vec::with_capacity(132);
            ret.extend_from_slice(SMT_KEY);
            ret.extend_from_slice(&[6]);
            ret.extend_from_slice(l_key.as_slice());
            ret.extend_from_slice(l_value.as_slice());
            ret.extend_from_slice(&[*l_height]);
            ret.extend_from_slice(r_key.as_slice());
            ret.extend_from_slice(r_value.as_slice());
            ret.extend_from_slice(&[*r_height]);
            ret
        }
        (
            MergeValue::MergeWithZero {
                base_node,
                zero_bits,
                zero_count,
            },
            MergeValue::ShortCut { key, value, height },
        ) => {
            let mut ret = Vec::with_capacity(132);
            ret.extend_from_slice(SMT_KEY);
            ret.extend_from_slice(&[7]);
            ret.extend_from_slice(base_node.as_slice());
            ret.extend_from_slice(zero_bits.as_slice());
            ret.extend_from_slice(&[*zero_count]);
            ret.extend_from_slice(key.as_slice());
            ret.extend_from_slice(value.as_slice());
            ret.extend_from_slice(&[*height]);
            ret
        }
        (
            MergeValue::ShortCut { key, value, height },
            MergeValue::MergeWithZero {
                base_node,
                zero_bits,
                zero_count,
            },
        ) => {
            let mut ret = Vec::with_capacity(132);
            ret.extend_from_slice(SMT_KEY);
            ret.extend_from_slice(&[8]);
            ret.extend_from_slice(key.as_slice());
            ret.extend_from_slice(value.as_slice());
            ret.extend_from_slice(&[*height]);
            ret.extend_from_slice(base_node.as_slice());
            ret.extend_from_slice(zero_bits.as_slice());
            ret.extend_from_slice(&[*zero_count]);
            ret
        }
    }
}

/// Deserialize a `BranchNode` from a slice that was previously serialized with `branch_node_to_vec`.
pub fn slice_to_branch_node(slice: &[u8]) -> BranchNode {
    match slice[1] {
        0 => {
            let left: [u8; 32] = slice[2..34].try_into().expect("checked slice");
            let right: [u8; 32] = slice[34..66].try_into().expect("checked slice");
            BranchNode {
                left: MergeValue::Value(left.into()),
                right: MergeValue::Value(right.into()),
            }
        }
        1 => {
            let left: [u8; 32] = slice[2..34].try_into().expect("checked slice");
            let base_node: [u8; 32] = slice[34..66].try_into().expect("checked slice");
            let zero_bits: [u8; 32] = slice[66..98].try_into().expect("checked slice");
            let zero_count = slice[98];
            BranchNode {
                left: MergeValue::Value(left.into()),
                right: MergeValue::MergeWithZero {
                    base_node: base_node.into(),
                    zero_bits: zero_bits.into(),
                    zero_count,
                },
            }
        }
        2 => {
            let base_node: [u8; 32] = slice[2..34].try_into().expect("checked slice");
            let zero_bits: [u8; 32] = slice[34..66].try_into().expect("checked slice");
            let zero_count = slice[66];
            let right: [u8; 32] = slice[67..99].try_into().expect("checked slice");
            BranchNode {
                left: MergeValue::MergeWithZero {
                    base_node: base_node.into(),
                    zero_bits: zero_bits.into(),
                    zero_count,
                },
                right: MergeValue::Value(right.into()),
            }
        }
        3 => {
            let l_base_node: [u8; 32] = slice[2..34].try_into().expect("checked slice");
            let l_zero_bits: [u8; 32] = slice[34..66].try_into().expect("checked slice");
            let l_zero_count = slice[66];
            let r_base_node: [u8; 32] = slice[67..99].try_into().expect("checked slice");
            let r_zero_bits: [u8; 32] = slice[99..131].try_into().expect("checked slice");
            let r_zero_count = slice[131];
            BranchNode {
                left: MergeValue::MergeWithZero {
                    base_node: l_base_node.into(),
                    zero_bits: l_zero_bits.into(),
                    zero_count: l_zero_count,
                },
                right: MergeValue::MergeWithZero {
                    base_node: r_base_node.into(),
                    zero_bits: r_zero_bits.into(),
                    zero_count: r_zero_count,
                },
            }
        }
        4 => {
            let left: [u8; 32] = slice[2..34].try_into().expect("checked slice");
            let key: [u8; 32] = slice[34..66].try_into().expect("checked slice");
            let value: [u8; 32] = slice[66..98].try_into().expect("checked slice");
            let height = slice[98];
            BranchNode {
                left: MergeValue::Value(left.into()),
                right: MergeValue::ShortCut {
                    key: key.into(),
                    value: value.into(),
                    height,
                },
            }
        }
        5 => {
            let key: [u8; 32] = slice[2..34].try_into().expect("checked slice");
            let value: [u8; 32] = slice[34..66].try_into().expect("checked slice");
            let height = slice[66];
            let right: [u8; 32] = slice[67..99].try_into().expect("checked slice");
            BranchNode {
                left: MergeValue::ShortCut {
                    key: key.into(),
                    value: value.into(),
                    height,
                },
                right: MergeValue::Value(right.into()),
            }
        }
        6 => {
            let l_key: [u8; 32] = slice[2..34].try_into().expect("checked slice");
            let l_value: [u8; 32] = slice[34..66].try_into().expect("checked slice");
            let l_height = slice[66];
            let r_key: [u8; 32] = slice[67..99].try_into().expect("checked slice");
            let r_value: [u8; 32] = slice[99..131].try_into().expect("checked slice");
            let r_height = slice[131];
            BranchNode {
                left: MergeValue::ShortCut {
                    key: l_key.into(),
                    value: l_value.into(),
                    height: l_height,
                },
                right: MergeValue::ShortCut {
                    key: r_key.into(),
                    value: r_value.into(),
                    height: r_height,
                },
            }
        }
        7 => {
            let base_node: [u8; 32] = slice[2..34].try_into().expect("checked slice");
            let zero_bits: [u8; 32] = slice[34..66].try_into().expect("checked slice");
            let zero_count = slice[66];
            let key: [u8; 32] = slice[67..99].try_into().expect("checked slice");
            let value: [u8; 32] = slice[99..131].try_into().expect("checked slice");
            let height = slice[131];
            BranchNode {
                left: MergeValue::MergeWithZero {
                    base_node: base_node.into(),
                    zero_bits: zero_bits.into(),
                    zero_count,
                },
                right: MergeValue::ShortCut {
                    key: key.into(),
                    value: value.into(),
                    height,
                },
            }
        }
        8 => {
            let key: [u8; 32] = slice[2..34].try_into().expect("checked slice");
            let value: [u8; 32] = slice[34..66].try_into().expect("checked slice");
            let height = slice[66];
            let base_node: [u8; 32] = slice[67..99].try_into().expect("checked slice");
            let zero_bits: [u8; 32] = slice[99..131].try_into().expect("checked slice");
            let zero_count = slice[131];
            BranchNode {
                left: MergeValue::ShortCut {
                    key: key.into(),
                    value: value.into(),
                    height,
                },
                right: MergeValue::MergeWithZero {
                    base_node: base_node.into(),
                    zero_bits: zero_bits.into(),
                    zero_count,
                },
            }
        }
        _ => {
            unreachable!()
        }
    }
}
