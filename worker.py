import time
import math
import requests
from decimal import Decimal, getcontext

BASE = "http://localhost:8099"
S = requests.Session()

# Decimal は標準ライブラリ。尾部の無限和を安定にするために少しだけ使う。
getcontext().prec = 80

def _frac(x: float) -> float:
    # x の小数部分（正に正規化）
    return x - math.floor(x)

def _powmod16(exp: int, mod: int) -> int:
    # 16^exp mod mod
    return pow(16, exp, mod)

def _bbp_S(j: int, n: int) -> float:
    """
    S_j(n) = sum_{k=0..n} 16^(n-k) mod (8k+j) / (8k+j)
           + sum_{k=n+1..∞} 16^(n-k) / (8k+j)
    ここで返すのは小数部分（厳密に整数部は不要）なので、途中で frac を取って暴走を防ぐ。
    """
    # 前半（mod で厳密に）
    s = 0.0
    for k in range(n + 1):
        ak = 8 * k + j
        r = _powmod16(n - k, ak)
        s = _frac(s + r / ak)

    # 後半（急速に減衰。必要項数だけ）
    # exp = n-k が負なので 16^(n-k) は 16^(-t) で急減。
    # 20～30項程度で十分になることが多い。nが大きい場合も尾部は同様に収束。
    t = 1
    while True:
        k = n + t
        ak = 8 * k + j
        term = (Decimal(16) ** Decimal(n - k)) / Decimal(ak)  # 16^(n-k)/ak
        term_f = float(term)
        if term_f < 1e-17:  # double 精度の下限目安
            break
        s = _frac(s + term_f)
        t += 1
        if t > 1000:  # 異常時の安全弁
            break

    return s

def pi_hex_digit(n: int) -> int:
    """
    π の16進小数点以下 n 桁目（0始まり）を返す（0..15）。
    digit = floor(16 * frac( 4*S1 - 2*S4 - S5 - S6 ))
    """
    s1 = _bbp_S(1, n)
    s4 = _bbp_S(4, n)
    s5 = _bbp_S(5, n)
    s6 = _bbp_S(6, n)

    x = 4.0 * s1 - 2.0 * s4 - 1.0 * s5 - 1.0 * s6
    x = _frac(x)
    d = int(16.0 * x)
    return d

def pi_hex_range(start: int, count: int) -> str:
    digits = []
    for i in range(count):
        d = pi_hex_digit(start + i)
        digits.append("0123456789ABCDEF"[d])
    return "".join(digits)

def do_job(payload: dict) -> dict:
    if payload.get("type") != "bbp_hex":
        raise ValueError(f"unknown job type: {payload.get('type')}")
    start = int(payload["start"])
    count = int(payload["count"])
    if start < 0 or count <= 0 or count > 512:
        raise ValueError("bad start/count (count<=64 recommended for demo)")

    hexstr = pi_hex_range(start, count)
    return {"hex": hexstr, "start": start, "count": count}

def main():
    while True:
        # time.sleep(1.0)

        r = S.get(f"{BASE}/job", timeout=10)
        if r.status_code == 204:
            time.sleep(1.0)
            continue
        r.raise_for_status()
        job = r.json()

        job_id = job["job_id"]
        payload = job["payload"]

        try:
            result = do_job(payload)
            S.post(f"{BASE}/result",
                   json={"job_id": job_id, "result": result},
                   timeout=20).raise_for_status()
        except Exception as e:
            # 既存の /fail がある前提（なければ /result にエラー格納でもOK）
            try:
                S.post(f"{BASE}/fail",
                       json={"job_id": job_id, "error": repr(e)},
                       timeout=10)
            except Exception:
                pass

if __name__ == "__main__":
    main()
