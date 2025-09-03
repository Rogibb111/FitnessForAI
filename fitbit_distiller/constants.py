from __future__ import annotations

# Candidate date/time column headers
DATE_COL_CANDIDATES = [
    "date", "date time", "date_time", "day",
    "start time", "start_time", "startdate", "start date",
    "log date", "log_date",
    "datetime", "time",
]

# Define normalized metric keys and their candidate column name fragments (lowercased)
METRIC_MAP = {
    "steps": ["steps", "step count"],
    "distance": ["distance"],  # unit unknown; keep raw numeric
    "calories": ["calories", "calorie"],
    "floors": ["floors"],
    "resting_heart_rate": ["resting heart rate", "restingheartrate", "resting_hr", "rhr"],
    "hrv_ms": ["rmssd", "hrv", "heart rate variability"],
    "spo2_percent": ["spo2", "oxygen saturation", "o2 saturation"],
    "sleep_duration_min": ["minutes asleep", "sleep duration", "time asleep", "sleep minutes"],
    "sleep_score": ["sleep score"],
    "readiness_score": ["readiness", "daily readiness"],
    "stress_score": ["stress score"],
    "skin_temp_variation": ["temperature variation", "temp variation", "temperature deviation"],
    "azm_minutes": ["active zone minutes", "azm"],
    "azm_fat_burn_minutes": ["fat burn minutes", "azm - fat burn", "active zone minutes - fat burn", "fat burn zone minutes", "fat burn"],
    "azm_cardio_minutes": ["cardio minutes", "azm - cardio", "active zone minutes - cardio", "cardio zone minutes", "cardio"],
    "azm_peak_minutes": ["peak minutes", "azm - peak", "active zone minutes - peak", "peak zone minutes", "peak"],
    "mindfulness_minutes": ["mindfulness minutes", "meditation minutes"],
    # Additional common daily metrics
    "lightly_active_minutes": ["lightly active minutes"],
    "fairly_active_minutes": ["fairly active minutes"],
    "very_active_minutes": ["very active minutes"],
    "sedentary_minutes": ["sedentary minutes"],
}

# For metrics that should be averaged rather than summed when multiple entries per day
AVERAGE_PREFERENCE = {"resting_heart_rate", "hrv_ms", "spo2_percent", "sleep_score", "readiness_score", "stress_score", "skin_temp_variation"}

# For metrics typically summed across rows (e.g., multiple logs in a day)
SUM_PREFERENCE = {
    "steps", "distance", "calories", "floors",
    "azm_minutes", "azm_fat_burn_minutes", "azm_cardio_minutes", "azm_peak_minutes",
    "mindfulness_minutes",
    "sleep_duration_min", "lightly_active_minutes", "fairly_active_minutes", "very_active_minutes", "sedentary_minutes",
    "workout_minutes", "workout_count",
}
