import matplotlib.pyplot as plt
import numpy as np

# Data
methods = ['Primary-Only', 'Closest', 'Pileus']
colors = {
    "Pileus": '#1b9e77',  # teal
    "Random": '#7570b3',  # purple
    "Closest": '#e6ab02',  # mustard
    "Primary-Only": '#d95f02',  # orange
}
no_burst = [0.999000, 0.627000, 0.992200]
burst = [0.532800, 0.627000, 0.645800]
burst_5 = [0, 0, 0.679700]
burst_1 = [0, 0, 0.714100]

# Bar width
bar_width = 0.2
index = np.arange(len(methods))  # The position of each group on the x-axis

# Plot
fig, ax = plt.subplots(figsize=(12, 8))

# Bars with different patterns for each method
bars_no_burst = ax.bar(index - bar_width, no_burst, bar_width, label='No Burst', color='#1b9e77', hatch='//')
bars_burst = ax.bar(index, burst, bar_width, label='Burst (Rep Interval=20s)', color='#7570b3', hatch='\\')
bars_burst_5 = ax.bar(index + bar_width, burst_5, bar_width, label='Burst (Rep Interval=5s)', color='#e6ab02', hatch='-')
bars_burst_1 = ax.bar(index + 2 * bar_width, burst_1, bar_width, label='Burst (Rep Interval=1s)', color='#d95f02', hatch='.')

# Labels and title
ax.set_ylabel('Utility', fontsize=18)
ax.set_xticks(index)
ax.set_xticklabels(methods, fontsize=18)
ax.tick_params(axis='x', labelsize=16)
ax.tick_params(axis='y', labelsize=16)

# ax.legend(loc='center left', bbox_to_anchor=(1.02, 0.5), ncol=1, fontsize=16)

ax.legend(
    loc='lower center',
    bbox_to_anchor=(0.5, 1),
    ncol=2,
    fontsize=16,
    frameon=False
)

# Grid
ax.grid(True, linestyle=':', linewidth=0.5)

# Show plot
plt.tight_layout()
plt.savefig("newfig.pdf", format='pdf')