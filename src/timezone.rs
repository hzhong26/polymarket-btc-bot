use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, TimeZone, Utc, Weekday};

const EST_OFFSET_SECS: i32 = 5 * 3600;
const EDT_OFFSET_SECS: i32 = 4 * 3600;

pub fn now_et() -> DateTime<FixedOffset> {
    to_et(Utc::now())
}

pub fn to_et(ts_utc: DateTime<Utc>) -> DateTime<FixedOffset> {
    ts_utc.with_timezone(&et_offset_for_utc(ts_utc))
}

pub fn et_abbreviation(ts_utc: DateTime<Utc>) -> &'static str {
    if is_us_eastern_dst(ts_utc) {
        "EDT"
    } else {
        "EST"
    }
}

pub fn et_offset_for_utc(ts_utc: DateTime<Utc>) -> FixedOffset {
    if is_us_eastern_dst(ts_utc) {
        FixedOffset::west_opt(EDT_OFFSET_SECS).expect("valid EDT offset")
    } else {
        FixedOffset::west_opt(EST_OFFSET_SECS).expect("valid EST offset")
    }
}

fn is_us_eastern_dst(ts_utc: DateTime<Utc>) -> bool {
    let year = ts_utc.year();
    let dst_start_utc = dst_start_utc(year);
    let dst_end_utc = dst_end_utc(year);
    ts_utc >= dst_start_utc && ts_utc < dst_end_utc
}

fn dst_start_utc(year: i32) -> DateTime<Utc> {
    // US DST starts at 2:00 AM local standard time on the second Sunday in March.
    let local_date = nth_weekday_of_month(year, 3, Weekday::Sun, 2);
    let local_naive = local_date
        .and_hms_opt(2, 0, 0)
        .expect("valid local DST start time");
    let est = FixedOffset::west_opt(EST_OFFSET_SECS).expect("valid EST offset");
    est.from_local_datetime(&local_naive)
        .single()
        .expect("unique EST local time")
        .with_timezone(&Utc)
}

fn dst_end_utc(year: i32) -> DateTime<Utc> {
    // US DST ends at 2:00 AM local daylight time on the first Sunday in November.
    let local_date = nth_weekday_of_month(year, 11, Weekday::Sun, 1);
    let local_naive = local_date
        .and_hms_opt(2, 0, 0)
        .expect("valid local DST end time");
    let edt = FixedOffset::west_opt(EDT_OFFSET_SECS).expect("valid EDT offset");
    edt.from_local_datetime(&local_naive)
        .single()
        .expect("unique EDT local time")
        .with_timezone(&Utc)
}

fn nth_weekday_of_month(year: i32, month: u32, weekday: Weekday, nth: u32) -> NaiveDate {
    let first = NaiveDate::from_ymd_opt(year, month, 1).expect("valid year-month");
    let first_idx = first.weekday().num_days_from_monday() as i32;
    let target_idx = weekday.num_days_from_monday() as i32;
    let delta_days = (7 + target_idx - first_idx) % 7;
    let day = 1 + delta_days as u32 + 7 * (nth.saturating_sub(1));
    NaiveDate::from_ymd_opt(year, month, day).expect("valid nth weekday date")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eastern_offset_handles_2026_dst_start_transition() {
        let before = "2026-03-08T06:59:59Z".parse::<DateTime<Utc>>().unwrap();
        let after = "2026-03-08T07:00:00Z".parse::<DateTime<Utc>>().unwrap();

        assert_eq!(et_offset_for_utc(before).local_minus_utc(), -5 * 3600);
        assert_eq!(et_offset_for_utc(after).local_minus_utc(), -4 * 3600);
    }

    #[test]
    fn eastern_offset_handles_2026_dst_end_transition() {
        let before = "2026-11-01T05:59:59Z".parse::<DateTime<Utc>>().unwrap();
        let after = "2026-11-01T06:00:00Z".parse::<DateTime<Utc>>().unwrap();

        assert_eq!(et_offset_for_utc(before).local_minus_utc(), -4 * 3600);
        assert_eq!(et_offset_for_utc(after).local_minus_utc(), -5 * 3600);
    }
}
