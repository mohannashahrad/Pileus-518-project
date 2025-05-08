import matplotlib.pyplot as plt

# Data
replication_5 = [
    0.6775, 0.6850, 0.6750, 0.6825, 0.6900, 0.6675, 0.6825, 0.6975, 0.6675, 0.6875,
    0.6700, 0.6700, 0.6750, 0.6900, 0.6650
]
replication_20 = [
    0.6725, 0.6800, 0.6575, 0.6700, 0.6775, 0.6600, 0.6725, 0.6850, 0.6600, 0.6850,
    0.6675, 0.6575, 0.6700, 0.6750, 0.6575
]
replication_20_coord = [
    0.6725, 0.6875, 0.6800, 0.6800, 0.6850, 0.6650, 0.6825, 0.6925, 0.6675, 0.6900,
    0.6700, 0.6675, 0.6750, 0.6850, 0.6725
]

# X-axis: session indices
sessions = list(range(1, len(replication_5) + 1))

# Plotting
plt.figure(figsize=(10, 6))
plt.plot(sessions, replication_5, marker='o', linestyle='--', label='Replication Interval 5s')
plt.plot(sessions, replication_20, marker='s', linestyle=':', label='Replication Interval 20s')
plt.plot(sessions, replication_20_coord, marker='^', linestyle='-', label='Replication Interval 20s + Reconfiguration')

# Labels and legend
plt.xlabel("Session Number", fontsize=16)
plt.ylabel("Utility", fontsize=16)
plt.ylim(0.65, 0.71)
plt.tick_params(axis='x', labelsize=14)
plt.tick_params(axis='y', labelsize=14)
plt.grid(True, linestyle='--', alpha=0.5)
plt.legend(fontsize=13)
plt.tight_layout()
plt.savefig('dynamic.pdf', format='pdf')



rep_interval = [20] * 100 + [10] * 100 + [5] * 400 +  [10] * 200 + [20] * 100


runs = list(range(len(rep_interval)))

plt.figure(figsize=(10, 4))
plt.plot(runs, rep_interval, marker='o', linestyle='-', color='black', markersize=3, linewidth=0.5)

plt.ylabel("Utah Replication Interval", fontsize=16)
plt.ylim(0, 25)
plt.grid(True, linestyle=':', linewidth=0.5)
plt.tick_params(axis='y', labelsize=1)
plt.xticks([])
plt.tight_layout()
plt.savefig("interval.pdf", format='pdf')
