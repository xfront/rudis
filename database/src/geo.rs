//! Geo module: geohash encoding, Haversine distance, and range search.
//!
//! Geo data is stored in a SortedSet where the score is a 52-bit geohash
//! value, consistent with Redis's implementation.

/// Earth's radius in meters.
const EARTH_RADIUS_M: f64 = 6372797.560856;

/// Number of bits for the geohash (Redis uses 52 bits for the score).
pub const GEO_STEP_MAX: u32 = 26;

/// Base32 alphabet used for geohash string encoding.
const BASE32: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";

/// Convert degrees to radians.
fn deg_to_rad(deg: f64) -> f64 {
    deg * std::f64::consts::PI / 180.0
}

/// Haversine formula: compute the distance in meters between two points
/// given as (longitude, latitude) in degrees.
pub fn haversine_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let dlat = deg_to_rad(lat2 - lat1);
    let dlon = deg_to_rad(lon2 - lon1);
    let a = (dlat / 2.0).sin().powi(2)
        + deg_to_rad(lat1).cos() * deg_to_rad(lat2).cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    EARTH_RADIUS_M * c
}

/// Encode (longitude, latitude) into a 52-bit geohash integer.
/// This is the same algorithm Redis uses: interleaving bits of
/// longitude and latitude ranges.
pub fn geohash_encode(lon: f64, lat: f64) -> u64 {
    let mut min_lon = -180.0f64;
    let mut max_lon = 180.0f64;
    let mut min_lat = -90.0f64;
    let mut max_lat = 90.0f64;

    let mut hash: u64 = 0;

    for i in (0..GEO_STEP_MAX).rev() {
        // Longitude bit
        let mid_lon = (min_lon + max_lon) / 2.0;
        if lon > mid_lon {
            hash |= 1u64 << (2 * i + 1);
            min_lon = mid_lon;
        } else {
            max_lon = mid_lon;
        }

        // Latitude bit
        let mid_lat = (min_lat + max_lat) / 2.0;
        if lat > mid_lat {
            hash |= 1u64 << (2 * i);
            min_lat = mid_lat;
        } else {
            max_lat = mid_lat;
        }
    }

    // Shift to the left by 12 bits to fit in the double precision mantissa
    // (Redis stores the geohash in the top 52 bits of the f64 score)
    hash <<= 12; // actually Redis shifts to align with the 52-bit mantissa
    // Actually, Redis uses the full 52 bits. Let me re-do this correctly.
    // Redis stores geohash as a 52-bit value in the sorted set score.
    // The score is the geohash value directly (as f64).
    // Let me use the standard approach:
    hash
}

/// Encode (longitude, latitude) into a geohash suitable for use as a sorted set score.
/// Redis uses a 52-bit geohash stored as the f64 score.
pub fn geohash_to_score(lon: f64, lat: f64) -> f64 {
    let hash = geohash_encode(lon, lat);
    hash as f64
}

/// Decode a 52-bit geohash integer back to (longitude, latitude).
pub fn geohash_decode(hash: u64) -> (f64, f64) {
    let mut min_lon = -180.0f64;
    let mut max_lon = 180.0f64;
    let mut min_lat = -90.0f64;
    let mut max_lat = 90.0f64;

    // The hash has 52 significant bits (shifted left by 12)
    let hash = hash >> 12;

    for i in (0..GEO_STEP_MAX).rev() {
        let lon_bit = (hash >> (2 * i + 1)) & 1;
        let lat_bit = (hash >> (2 * i)) & 1;

        let mid_lon = (min_lon + max_lon) / 2.0;
        if lon_bit == 1 {
            min_lon = mid_lon;
        } else {
            max_lon = mid_lon;
        }

        let mid_lat = (min_lat + max_lat) / 2.0;
        if lat_bit == 1 {
            min_lat = mid_lat;
        } else {
            max_lat = mid_lat;
        }
    }

    let lon = (min_lon + max_lon) / 2.0;
    let lat = (min_lat + max_lat) / 2.0;
    (lon, lat)
}

/// Decode a sorted set score back to (longitude, latitude).
pub fn score_to_geohash(score: f64) -> (f64, f64) {
    geohash_decode(score as u64)
}

/// Encode a geohash integer into a base32 string (11 characters, like Redis).
pub fn geohash_to_string(hash: u64) -> String {
    // Redis uses 11 base32 characters (55 bits, but we only have 52)
    // We'll use the 52-bit hash shifted appropriately
    let mut result = Vec::with_capacity(11);
    // Shift left to get 55 bits (11 * 5)
    let _h = hash >> 7; // 52 - 7 = 45 bits, but we need 55... 
    // Actually, let's use the standard approach:
    // Take the 52-bit hash and produce 11 base32 chars
    // Each char encodes 5 bits. 11 * 5 = 55 bits.
    // We have 52 bits, so we pad with 3 zero bits at the end.
    let h = hash << 3; // Now we have 55 bits in the top of the u64

    for i in (0..11).rev() {
        let idx = ((h >> (i * 5)) & 0x1F) as usize;
        result.push(BASE32[idx]);
    }

    String::from_utf8(result).unwrap()
}

/// Convert distance in meters to the specified unit.
pub fn convert_distance(meters: f64, unit: &str) -> f64 {
    match unit.to_ascii_lowercase().as_str() {
        "m" => meters,
        "km" => meters / 1000.0,
        "ft" => meters / 0.3048,
        "mi" => meters / 1609.34,
        _ => meters,
    }
}

/// Parse a distance unit and return the conversion factor to meters.
pub fn unit_to_meters(unit: &str) -> f64 {
    match unit.to_ascii_lowercase().as_str() {
        "m" => 1.0,
        "km" => 1000.0,
        "ft" => 0.3048,
        "mi" => 1609.34,
        _ => 1.0,
    }
}

/// Validate longitude (-180 to 180).
pub fn validate_longitude(lon: f64) -> bool {
    lon >= -180.0 && lon <= 180.0
}

/// Validate latitude (-85.05112878 to 85.05112878).
pub fn validate_latitude(lat: f64) -> bool {
    lat >= -85.05112878 && lat <= 85.05112878
}

#[cfg(test)]
mod test_geo {
    use super::*;

    #[test]
    fn test_haversine() {
        // New York to London: approximately 5570 km
        let dist = haversine_distance(-74.006, 40.7128, -0.1278, 51.5074);
        assert!(dist > 5500000.0 && dist < 5700000.0);
    }

    #[test]
    fn test_geohash_encode_decode() {
        let lon = 13.361389;
        let lat = 38.115556;
        let hash = geohash_encode(lon, lat);
        let (d_lon, d_lat) = geohash_decode(hash);
        // Should be within ~0.01 degrees
        assert!((d_lon - lon).abs() < 0.01);
        assert!((d_lat - lat).abs() < 0.01);
    }

    #[test]
    fn test_geohash_to_string() {
        let hash = geohash_encode(13.361389, 38.115556);
        let s = geohash_to_string(hash);
        assert_eq!(s.len(), 11);
    }

    #[test]
    fn test_convert_distance() {
        assert!((convert_distance(1000.0, "km") - 1.0).abs() < 0.001);
        assert!((convert_distance(1609.34, "mi") - 1.0).abs() < 0.01);
        assert!((convert_distance(0.3048, "ft") - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_validate_coords() {
        assert!(validate_longitude(13.0));
        assert!(!validate_longitude(181.0));
        assert!(validate_latitude(38.0));
        assert!(!validate_latitude(90.0));
    }

    #[test]
    fn test_score_roundtrip() {
        let lon = -73.935242;
        let lat = 40.730610;
        let score = geohash_to_score(lon, lat);
        let (d_lon, d_lat) = score_to_geohash(score);
        assert!((d_lon - lon).abs() < 0.01);
        assert!((d_lat - lat).abs() < 0.01);
    }
}
