# engine/ai_blender.py
import math
from typing import Optional

def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def smooth_confidence(prev_conf: Optional[float], new_conf: Optional[float], alpha: float = 0.25) -> Optional[float]:
    """
    Simple EWMA smoothing to avoid wild confidence jumps.
    prev_conf: previous confidence in 0..1 or None
    new_conf: new confidence from AI
    alpha: smoothing factor 0..1
    """
    if new_conf is None:
        return prev_conf
    new_conf = clamp(float(new_conf), 0.0, 1.0)
    if prev_conf is None:
        return new_conf
    return prev_conf * (1 - alpha) + new_conf * alpha

def safe_confidence_for_risk(confidence: Optional[float]) -> Optional[float]:
    """
    Apply conservative mapping: extreme values (0 or 1) are softened.
    This prevents large multipliers in the risk engine.
    """
    if confidence is None:
        return None
    c = clamp(confidence, 0.0, 1.0)
    # reduce extremes: map [0,1] -> [0.05, 0.95] so multipliers are kept in check
    return 0.05 + 0.90 * c
