import re

def parse_log_and_compute_avg_rtt(log_path):
    rtt_values_us = []

    pattern = re.compile(r'rtt is ([\d.]+)([µu]s|ms)')  # Match RTT and its unit

    with open(log_path, 'r') as file:
        for line in file:
            if "and rtt is" in line:
                match = pattern.search(line)
                if match:
                    value = float(match.group(1))
                    unit = match.group(2)

                    # Normalize to microseconds
                    if unit == 'ms':
                        value *= 1000
                    elif unit in ('µs', 'us'):  # µs = microsecond symbol, us = fallback
                        pass
                    else:
                        print(f"Unknown unit in line: {line.strip()}")
                        continue

                    rtt_values_us.append(value)

    if rtt_values_us:
        avg_rtt = sum(rtt_values_us) / len(rtt_values_us)
        print(f"Average RTT: {avg_rtt:.2f} µs over {len(rtt_values_us)} samples")
    else:
        print("No RTT values found.")

# Example usage
parse_log_and_compute_avg_rtt("../../client/fig11_skew_pileus_07.log")
