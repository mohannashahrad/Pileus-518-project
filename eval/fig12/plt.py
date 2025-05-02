import matplotlib.pyplot as plt
import numpy as np

# Data
regions = ["Utah", "Clemson", "Frankfurt", "Tokyo"]
modes = ["Primary", "Random", "Closest", "Pileus"]
colors = {
    "Pileus": "#1f77b4",   # blue
    "Random": "#ff7f0e",   # orange
    "Closest": "#2ca02c",  # green
    "Primary": "#d62728",  # red
}

# Utilities per region
data = {
    "Tokyo":    {"Closest": 0.005100, "Random": 0.079345, "Primary": 0.249756, "Pileus": 0.249756},
    "Utah":     {"Closest": 0.500000, "Random": 0.243538, "Primary": 0.996103, "Pileus": 0.999157},
    "Clemson":  {"Closest": 1.000000, "Random": 0.320951, "Primary": 1.000000, "Pileus": 1.000000},
    "Frankfurt":{"Closest": 0.500000, "Random": 0.224625, "Primary": 0.250000, "Pileus": 0.500000},
}

# Plot setup
x = np.arange(len(regions))
width = 0.2  # width of each bar
offsets = [-1.5, -0.5, 0.5, 1.5]  # for 4 bars per region

fig, ax = plt.subplots(figsize=(10, 6))

# Plot each mode
for i, mode in enumerate(modes):
    values = [data[region][mode.capitalize()] for region in regions]
    ax.bar(x + offsets[i]*width, values, width, label=mode, color=colors[mode])

# Labels and legend
ax.set_ylabel("Average Utility")
ax.set_xticks(x)
ax.set_xticklabels(regions)
ax.legend(title="Mode")

plt.title("Average Utility per Region per Mode")
plt.tight_layout()
plt.savefig('fig12.pdf', format='pdf')

