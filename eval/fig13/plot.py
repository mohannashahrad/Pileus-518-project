import re
import matplotlib.pyplot as plt

def extract_matching_lines(log_file_path, output_file_path="tmp.log"):
    with open(log_file_path, 'r') as infile, open(output_file_path, 'w') as outfile:
        for line in infile:
            if "chosen subsla is" in line or "subSLAGained" in line:
                clean_line = line.strip()
                print(clean_line)
                outfile.write(clean_line + '\n')

def extract_subsla_utilities(filepath):
    utilities = []
    pattern = re.compile(r"subSLAGained=\{.*? ([0-9.]+)\}")
    err_pattern = re.compile(r"subSLAGained: \{.*? ([0-9.]+)\}")

    with open(filepath, 'r') as f:
        for line in f:
            match = pattern.search(line)
            err_match = err_pattern.search(line)
            if match:
                utilities.append(float(match.group(1)))
            if err_match:
                utilities.append(float(err_match.group(1)))

    return utilities

# extract_matching_lines("fig13.log")
utilities = extract_subsla_utilities("tmp.log")

runs = list(range(len(utilities)))

plt.figure(figsize=(10, 4))
plt.plot(runs, utilities, marker='o', linestyle='-', color='black', label='Utility per read', markersize=3, linewidth=0.5)

plt.xlabel("Run #")
plt.ylabel("Utility")
plt.ylim(-0.1, 1.1)
plt.grid(True, linestyle=':', linewidth=0.5)
plt.legend()
plt.tight_layout()
plt.savefig("fig13.pdf", format='pdf')


