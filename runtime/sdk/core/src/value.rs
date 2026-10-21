/* Public Talon value contract. Copyright (c) 2026 Talon Contributors. */
use crate::Error;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::str::FromStr;
/// Maximum precision supported by Talon's fixed-point decimal representation.
/// `10^38 - 1` fits in the signed `i128` coefficient used on disk.
pub const DECIMAL_MAX_PRECISION: u8 = 38;

/// Exact fixed-point decimal. `coefficient * 10^-scale` is the represented value.
///
/// Scale is intentionally retained (rather than normalized away) so values read
/// from a `DECIMAL(p,s)` column preserve that column's display contract. Numeric
/// equality and ordering ignore representation-only trailing zeroes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Decimal {
    coefficient: i128,
    scale: u8,
}

impl Decimal {
    pub const fn from_parts(coefficient: i128, scale: u8) -> Self {
        Self { coefficient, scale }
    }

    pub fn parse(input: &str) -> Result<Self, Error> {
        let input = input.trim();
        if input.is_empty() {
            return Err(Error::Serialization("empty decimal literal".into()));
        }
        let (mantissa, exponent) = match input.rfind(['e', 'E']) {
            Some(pos) => {
                let exponent = input[pos + 1..].parse::<i32>().map_err(|_| {
                    Error::Serialization(format!("invalid decimal literal: {input}"))
                })?;
                (&input[..pos], exponent)
            }
            None => (input, 0),
        };
        let (negative, unsigned) = match mantissa.as_bytes().first() {
            Some(b'-') => (true, &mantissa[1..]),
            Some(b'+') => (false, &mantissa[1..]),
            _ => (false, mantissa),
        };
        let mut digits = String::with_capacity(unsigned.len());
        let mut fraction_digits = 0i32;
        let mut seen_dot = false;
        for ch in unsigned.chars() {
            match ch {
                '0'..='9' => {
                    digits.push(ch);
                    if seen_dot {
                        fraction_digits += 1;
                    }
                }
                '.' if !seen_dot => seen_dot = true,
                _ => {
                    return Err(Error::Serialization(format!(
                        "invalid decimal literal: {input}"
                    )))
                }
            }
        }
        if digits.is_empty() {
            return Err(Error::Serialization(format!(
                "invalid decimal literal: {input}"
            )));
        }
        let mut scale = fraction_digits - exponent;
        if scale < 0 {
            let zeros = (-scale) as u32;
            if zeros > DECIMAL_MAX_PRECISION as u32 {
                return Err(Error::Serialization(format!("decimal overflow: {input}")));
            }
            digits.extend(std::iter::repeat('0').take(zeros as usize));
            scale = 0;
        }
        if scale > DECIMAL_MAX_PRECISION as i32 {
            return Err(Error::Serialization(format!(
                "decimal scale exceeds {}: {input}",
                DECIMAL_MAX_PRECISION
            )));
        }
        let digits_trimmed = digits.trim_start_matches('0');
        if digits_trimmed.len() > DECIMAL_MAX_PRECISION as usize {
            return Err(Error::Serialization(format!("decimal overflow: {input}")));
        }
        let magnitude = if digits_trimmed.is_empty() {
            0
        } else {
            i128::from_str(digits_trimmed)
                .map_err(|_| Error::Serialization(format!("decimal overflow: {input}")))?
        };
        let coefficient = if negative {
            magnitude
                .checked_neg()
                .ok_or_else(|| Error::Serialization(format!("decimal overflow: {input}")))?
        } else {
            magnitude
        };
        Ok(Self::from_parts(coefficient, scale as u8))
    }

    pub const fn coefficient(self) -> i128 {
        self.coefficient
    }

    pub const fn scale(self) -> u8 {
        self.scale
    }

    /// Canonical representation for equality-sensitive keys and hashes.
    /// Column values themselves retain their declared display scale.
    pub fn normalized(self) -> Self {
        let mut coefficient = self.coefficient;
        let mut scale = self.scale;
        while scale > 0 && coefficient % 10 == 0 {
            coefficient /= 10;
            scale -= 1;
        }
        Self::from_parts(coefficient, scale)
    }

    pub fn precision(self) -> u8 {
        decimal_digits(self.coefficient)
    }

    pub fn quantize(self, target_scale: u8) -> Result<Self, Error> {
        if target_scale > DECIMAL_MAX_PRECISION {
            return Err(Error::Serialization(format!(
                "decimal scale exceeds {}",
                DECIMAL_MAX_PRECISION
            )));
        }
        match target_scale.cmp(&self.scale) {
            Ordering::Equal => Ok(self),
            Ordering::Greater => {
                let factor = pow10_i128(target_scale - self.scale)?;
                let coefficient = self
                    .coefficient
                    .checked_mul(factor)
                    .ok_or_else(|| Error::Serialization("decimal overflow while scaling".into()))?;
                decimal_result(coefficient, target_scale, "decimal overflow while scaling")
            }
            Ordering::Less => {
                let divisor = pow10_i128(self.scale - target_scale)?;
                let quotient = self.coefficient / divisor;
                let remainder = self.coefficient % divisor;
                let rounded = if remainder.abs() >= (divisor / 2) {
                    quotient
                        .checked_add(self.coefficient.signum())
                        .ok_or_else(|| {
                            Error::Serialization("decimal overflow while rounding".into())
                        })?
                } else {
                    quotient
                };
                decimal_result(rounded, target_scale, "decimal overflow while rounding")
            }
        }
    }

    pub fn fits(self, precision: u8, scale: u8) -> bool {
        precision > 0
            && precision <= DECIMAL_MAX_PRECISION
            && scale <= precision
            && self.scale == scale
            && self.precision() <= precision
    }

    fn align(self, rhs: Self) -> Result<(i128, i128, u8), Error> {
        let scale = self.scale.max(rhs.scale);
        let left = self
            .coefficient
            .checked_mul(pow10_i128(scale - self.scale)?)
            .ok_or_else(|| Error::SqlExec("decimal overflow while aligning operands".into()))?;
        let right = rhs
            .coefficient
            .checked_mul(pow10_i128(scale - rhs.scale)?)
            .ok_or_else(|| Error::SqlExec("decimal overflow while aligning operands".into()))?;
        Ok((left, right, scale))
    }

    pub fn checked_add(self, rhs: Self) -> Result<Self, Error> {
        let (left, right, scale) = self.align(rhs)?;
        let coefficient = left
            .checked_add(right)
            .ok_or_else(|| Error::SqlExec("decimal addition overflow".into()))?;
        decimal_result(coefficient, scale, "decimal addition overflow")
    }

    pub fn checked_sub(self, rhs: Self) -> Result<Self, Error> {
        let (left, right, scale) = self.align(rhs)?;
        let coefficient = left
            .checked_sub(right)
            .ok_or_else(|| Error::SqlExec("decimal subtraction overflow".into()))?;
        decimal_result(coefficient, scale, "decimal subtraction overflow")
    }

    pub fn checked_mul(self, rhs: Self) -> Result<Self, Error> {
        let scale = self
            .scale
            .checked_add(rhs.scale)
            .filter(|s| *s <= DECIMAL_MAX_PRECISION)
            .ok_or_else(|| Error::SqlExec("decimal multiplication scale overflow".into()))?;
        let coefficient = self
            .coefficient
            .checked_mul(rhs.coefficient)
            .ok_or_else(|| Error::SqlExec("decimal multiplication overflow".into()))?;
        decimal_result(coefficient, scale, "decimal multiplication overflow")
    }

    /// Exact quotient rounded half away from zero to `result_scale`.
    pub fn checked_div(self, rhs: Self, result_scale: u8) -> Result<Self, Error> {
        if rhs.coefficient == 0 {
            return Err(Error::SqlExec("division by zero".into()));
        }
        if result_scale > DECIMAL_MAX_PRECISION {
            return Err(Error::SqlExec("decimal division scale overflow".into()));
        }
        let exponent = result_scale as i16 + rhs.scale as i16 - self.scale as i16;
        let numerator = self.coefficient.unsigned_abs();
        let denominator = rhs.coefficient.unsigned_abs();
        let (mut quotient, remainder, rounding_denominator) = if exponent >= 0 {
            let (quotient, remainder) =
                div_scaled_by_power10(numerator, denominator, exponent as u8)?;
            (quotient, remainder, Some(denominator))
        } else {
            let factor = pow10_u128((-exponent) as u8)?;
            match denominator.checked_mul(factor) {
                Some(scaled_denominator) => (
                    numerator / scaled_denominator,
                    numerator % scaled_denominator,
                    Some(scaled_denominator),
                ),
                // denominator * 10^k exceeds u128::MAX, while numerator is at
                // most 10^38-1. The exact quotient is below 0.5, so it rounds
                // to zero at the requested scale.
                None => (0, numerator, None),
            }
        };
        if rounding_denominator.is_some_and(|d| remainder * 2 >= d) {
            quotient = quotient
                .checked_add(1)
                .ok_or_else(|| Error::SqlExec("decimal division overflow".into()))?;
        }
        let coefficient = i128::try_from(quotient)
            .map_err(|_| Error::SqlExec("decimal division overflow".into()))?;
        let coefficient = if self.coefficient.signum() == rhs.coefficient.signum() {
            coefficient
        } else {
            -coefficient
        };
        decimal_result(coefficient, result_scale, "decimal division overflow")
    }

    pub fn numeric_cmp(self, rhs: Self) -> Ordering {
        if let Ok((left, right, _)) = self.align(rhs) {
            return left.cmp(&right);
        }
        // Alignment overflow can only happen for very different scales. Compare
        // sign, adjusted exponent and zero-padded significands without floats.
        decimal_cmp_text(self, rhs)
    }
}

impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        self.numeric_cmp(*other) == Ordering::Equal
    }
}

impl Eq for Decimal {}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.numeric_cmp(*other))
    }
}

impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> Ordering {
        self.numeric_cmp(*other)
    }
}

impl std::fmt::Display for Decimal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let negative = self.coefficient < 0;
        let digits = self.coefficient.unsigned_abs().to_string();
        if negative {
            f.write_str("-")?;
        }
        if self.scale == 0 {
            return f.write_str(&digits);
        }
        let scale = self.scale as usize;
        if digits.len() <= scale {
            f.write_str("0.")?;
            for _ in 0..(scale - digits.len()) {
                f.write_str("0")?;
            }
            f.write_str(&digits)
        } else {
            let split = digits.len() - scale;
            write!(f, "{}.{}", &digits[..split], &digits[split..])
        }
    }
}

fn decimal_digits(value: i128) -> u8 {
    if value == 0 {
        1
    } else {
        value.unsigned_abs().to_string().len() as u8
    }
}

fn pow10_i128(power: u8) -> Result<i128, Error> {
    if power > DECIMAL_MAX_PRECISION {
        return Err(Error::Serialization("decimal scale overflow".into()));
    }
    10i128
        .checked_pow(power as u32)
        .ok_or_else(|| Error::Serialization("decimal scale overflow".into()))
}

fn pow10_u128(power: u8) -> Result<u128, Error> {
    if power > DECIMAL_MAX_PRECISION {
        return Err(Error::SqlExec("decimal division scale overflow".into()));
    }
    10u128
        .checked_pow(power as u32)
        .ok_or_else(|| Error::SqlExec("decimal division scale overflow".into()))
}

fn decimal_result(coefficient: i128, scale: u8, message: &str) -> Result<Decimal, Error> {
    if scale > DECIMAL_MAX_PRECISION || decimal_digits(coefficient) > DECIMAL_MAX_PRECISION {
        return Err(Error::SqlExec(message.into()));
    }
    Ok(Decimal::from_parts(coefficient, scale))
}

/// Divide `numerator * 10^power` by `denominator` without ever materializing
/// the widened numerator. Long division keeps only the final quotient and the
/// current remainder, so every representable 38-digit result is accepted.
fn div_scaled_by_power10(
    numerator: u128,
    denominator: u128,
    power: u8,
) -> Result<(u128, u128), Error> {
    let mut quotient = numerator / denominator;
    let mut remainder = numerator % denominator;
    for _ in 0..power {
        let (digit, next_remainder) = next_decimal_division_digit(remainder, denominator);
        quotient = quotient
            .checked_mul(10)
            .and_then(|q| q.checked_add(digit as u128))
            .filter(|q| *q <= 10u128.pow(DECIMAL_MAX_PRECISION as u32) - 1)
            .ok_or_else(|| Error::SqlExec("decimal division overflow".into()))?;
        remainder = next_remainder;
    }
    Ok((quotient, remainder))
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct WideU128 {
    hi: u128,
    lo: u128,
}

fn wide_mul_small(value: u128, multiplier: u8) -> WideU128 {
    let low = (value as u64) as u128;
    let high = (value >> 64) as u64 as u128;
    let low_product = low * multiplier as u128;
    let high_product = high * multiplier as u128;
    let (lo, carry) = low_product.overflowing_add(high_product << 64);
    WideU128 {
        hi: (high_product >> 64) + u128::from(carry),
        lo,
    }
}

fn wide_sub(left: WideU128, right: WideU128) -> WideU128 {
    let (lo, borrow) = left.lo.overflowing_sub(right.lo);
    WideU128 {
        hi: left.hi - right.hi - u128::from(borrow),
        lo,
    }
}

fn next_decimal_division_digit(remainder: u128, denominator: u128) -> (u8, u128) {
    let scaled_remainder = wide_mul_small(remainder, 10);
    for digit in (0..=9).rev() {
        let product = wide_mul_small(denominator, digit);
        if product <= scaled_remainder {
            let next = wide_sub(scaled_remainder, product);
            debug_assert_eq!(next.hi, 0);
            debug_assert!(next.lo < denominator);
            return (digit, next.lo);
        }
    }
    unreachable!("zero always fits into an unsigned division remainder")
}

fn decimal_cmp_text(left: Decimal, right: Decimal) -> Ordering {
    if left.coefficient.signum() != right.coefficient.signum() {
        return left.coefficient.signum().cmp(&right.coefficient.signum());
    }
    if left.coefficient == 0 && right.coefficient == 0 {
        return Ordering::Equal;
    }
    let left_digits = left.coefficient.unsigned_abs().to_string();
    let right_digits = right.coefficient.unsigned_abs().to_string();
    let left_exp = left_digits.len() as i16 - left.scale as i16;
    let right_exp = right_digits.len() as i16 - right.scale as i16;
    let magnitude = left_exp.cmp(&right_exp).then_with(|| {
        let width = left_digits.len().max(right_digits.len());
        left_digits
            .bytes()
            .chain(std::iter::repeat(b'0'))
            .take(width)
            .cmp(
                right_digits
                    .bytes()
                    .chain(std::iter::repeat(b'0'))
                    .take(width),
            )
    });
    if left.coefficient < 0 {
        magnitude.reverse()
    } else {
        magnitude
    }
}
/// 单值类型（v2 规格：INTEGER/FLOAT/TEXT/BLOB/BOOLEAN/JSONB/VECTOR/TIMESTAMP/GEOPOINT）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum Value {
    #[default]
    Null,
    Integer(i64),
    Float(f64),
    /// Exact fixed-point decimal. Never represented as IEEE-754 in storage.
    Decimal(Decimal),
    Text(String),
    Blob(Vec<u8>),
    Boolean(bool),
    Jsonb(serde_json::Value),
    /// 向量；维度由 Schema 约束，单维上限见 `VECTOR_DIM_MAX`。
    Vector(Vec<f32>),
    /// 时间戳；语义由使用方约定（通常为自纪元以来的毫秒或微秒）。
    Timestamp(i64),
    /// M94：地理坐标 (latitude, longitude)，WGS-84。
    GeoPoint(f64, f64),
    /// 日期类型：自 Unix 纪元 (1970-01-01) 以来的天数（i32，可表示负值即纪元前）。
    /// 对标 Apache DataFusion Date32 / SQL DATE。
    Date(i32),
    /// 时间类型：自午夜以来的纳秒数（i64，0..86_399_999_999_999）。
    /// 对标 Apache DataFusion Time64(Nanosecond) / SQL TIME。
    Time(i64),
    /// 参数化查询占位符 `?`，存储 0-based 参数索引。
    /// 解析阶段生成，执行前必须通过 `bind_params` 替换为实际值。
    #[serde(skip)]
    Placeholder(usize),
}
/// 将自 1970-01-01 以来的天数转换为 `YYYY-MM-DD` 字符串。
pub fn date_to_string(days: i32) -> String {
    let z = days as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02}", y, m, d)
}

/// 将自午夜以来的纳秒数转换为 `HH:MM:SS.nnnnnnnnn` 字符串。
pub fn time_to_string(nanos: i64) -> String {
    let h = nanos / 3_600_000_000_000;
    let rem = nanos % 3_600_000_000_000;
    let m = rem / 60_000_000_000;
    let rem = rem % 60_000_000_000;
    let s = rem / 1_000_000_000;
    let ns = rem % 1_000_000_000;
    if ns == 0 {
        format!("{:02}:{:02}:{:02}", h, m, s)
    } else {
        format!("{:02}:{:02}:{:02}.{:09}", h, m, s, ns)
    }
}
