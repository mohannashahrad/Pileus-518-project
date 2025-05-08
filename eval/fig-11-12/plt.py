import matplotlib.pyplot as plt
import numpy as np

# Data
regions = ["Utah", "Clemson", "Frankfurt", "Tokyo"]
modes = ["Primary", "Random", "Closest", "Pileus"]
colors = {
    "Pileus": '#1b9e77',  # teal
    "Random": '#7570b3',  # purple
    "Closest": '#e6ab02',  # mustard
    "Primary": '#d95f02',  # orange
}
patterns = {
    "Pileus": "/", 
    "Random": ".",  
    "Closest": "+",  
    "Primary": "x",  
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
ax.set_ylabel("Average Utility", fontsize=18)
ax.set_xlabel("Region", fontsize=18) 
ax.set_xticks(x)
ax.set_xticklabels(regions, fontsize=14)  
ax.tick_params(axis='y', labelsize=14)
ax.legend(fontsize=16, title_fontsize=16)

plt.tight_layout()
plt.savefig('fig12.pdf', format='pdf')


# Reproduce Fig 11

# Utilities per region
data = {
    "Tokyo":    {"Closest": 0.997522, "Random": 0.334089, "Primary": 0.000000, "Pileus": 0.999024},
    "Utah":     {"Closest": 0.996959, "Random":  0.994865, "Primary": 1.000000, "Pileus": 1.000000},
    "Clemson":  {"Closest": 1.000000 , "Random": 0.649274 , "Primary": 1.000000 , "Pileus": 1.000000},
    "Frankfurt":{"Closest": 0.998569, "Random": 0.676893, "Primary": 0.000000, "Pileus": 0.998569},
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
ax.set_ylabel("Average Utility", fontsize=18)
ax.set_xlabel("Region", fontsize=18) 
ax.set_xticks(x)
ax.set_xticklabels(regions, fontsize=14)  
ax.tick_params(axis='y', labelsize=14)
ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.15), ncol=4, fontsize=16)
plt.tight_layout()
plt.savefig('fig11.pdf', format='pdf')

# Skew plot 
skew = [0.2, 0.4, 0.6, 0.8]

# Utility values
pileus_utility = [0.909000, 0.828500, 0.745500, 0.692000]
closest_utility = [0.908000, 0.829000, 0.751000, 0.696000]

# Plotting
plt.figure(figsize=(8, 5))
plt.plot(skew, closest_utility, marker='s', linestyle='--', color='black')

plt.xlabel('Workload Skew', fontsize=16)
plt.ylabel('Average Utility',fontsize=16)
ax.tick_params(axis='x', labelsize=14)
ax.tick_params(axis='y', labelsize=14)
plt.grid(True)
plt.tight_layout()
plt.savefig('skew.pdf', format='pdf')


# Data
rep = [0.5, 1, 5, 10, 20, 30, 60]
pileus_utility = [0.847000, 0.8395, 0.8285, 0.8250, 0.8185, 0.8110, 0.801500]

# Plot
fig, ax = plt.subplots(figsize=(8, 5))
ax.plot(rep, pileus_utility, marker='s', linestyle='--', color='black', label='Pileus Utility')

# Labels and title
ax.set_xlabel('Replication Interval (seconds)', fontsize=16)
ax.set_ylabel('Average Utility', fontsize=16)
ax.tick_params(axis='x', labelsize=14)
ax.tick_params(axis='y', labelsize=14)
ax.grid(True)
ax.legend(fontsize=14)

# Finalize
plt.tight_layout()
plt.savefig('repFreq.pdf', format='pdf')

# Plot Frankfurt RTT
skew = [0.2, 0.3, 0.4, 0.5, 0.6, 0.8]
pileus_utility = [0.99, 0.99, 0.99, 0.99, 0.97, 0.97]
rtt = [21.559, 32.049, 38.408, 44.309, 60.259, 72.250]

# Plot
fig, ax = plt.subplots(figsize=(8, 5))
ax.plot(skew, rtt, marker='s', linestyle='--', color='black', label='Pileus Utility')

# Labels and title
ax.set_xlabel('Workload Skew Factor', fontsize=16)
ax.set_ylabel('Average RTT of Read Operations (MS)', fontsize=16)
ax.tick_params(axis='x', labelsize=14)
ax.tick_params(axis='y', labelsize=14)
ax.grid(True)
ax.legend(fontsize=14)

# Finalize
plt.tight_layout()
plt.savefig('rtt.pdf', format='pdf')

