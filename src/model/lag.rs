#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LagFlag {
    None,
    NoCommit,
    Expired,
    Reset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lag {
    pub value: Option<i64>,
    pub flag: LagFlag,
}

pub fn compute_lag(low: i64, high: i64, committed: Option<i64>) -> Lag {
    let Some(committed) = committed else {
        return Lag {
            value: None,
            flag: LagFlag::NoCommit,
        };
    };
    if committed > high {
        return Lag {
            value: Some(0),
            flag: LagFlag::Reset,
        };
    }
    if committed < low {
        return Lag {
            value: Some((high - low).max(0)),
            flag: LagFlag::Expired,
        };
    }
    Lag {
        value: Some(high - committed),
        flag: LagFlag::None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_commit_has_no_lag_value() {
        assert_eq!(
            compute_lag(0, 10, None),
            Lag {
                value: None,
                flag: LagFlag::NoCommit
            }
        );
    }

    #[test]
    fn normal_lag_is_high_minus_committed() {
        assert_eq!(compute_lag(0, 10, Some(4)).value, Some(6));
        assert_eq!(compute_lag(0, 10, Some(4)).flag, LagFlag::None);
    }

    #[test]
    fn caught_up_is_zero() {
        assert_eq!(compute_lag(3, 10, Some(10)).value, Some(0));
    }

    #[test]
    fn empty_partition_with_commit_at_zero() {
        assert_eq!(compute_lag(0, 0, Some(0)).value, Some(0));
    }

    #[test]
    fn expired_commit_counts_from_low_watermark() {
        let lag = compute_lag(50, 80, Some(20));
        assert_eq!(lag.value, Some(30));
        assert_eq!(lag.flag, LagFlag::Expired);
    }

    #[test]
    fn commit_above_high_clamps_to_zero_with_reset_flag() {
        let lag = compute_lag(0, 5, Some(9));
        assert_eq!(lag.value, Some(0));
        assert_eq!(lag.flag, LagFlag::Reset);
    }
}
