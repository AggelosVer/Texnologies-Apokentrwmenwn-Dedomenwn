import json

notebook_path = r'c:\Users\User\Desktop\GitHub\Texnologies-Apokentrwmenwn-Dedomenwn\analysis_notebook.ipynb'

with open(notebook_path, 'r', encoding='utf-8') as f:
    nb = json.load(f)

# Fix IndentationError in the visualization cell
for cell in nb['cells']:
    if cell['cell_type'] == 'code' and 'Visualize hash distribution' in ''.join(cell['source']):
        source = cell['source']
        new_source = []
        for line in source:
            # Check if this is the problematic line and fix indentation
            if "plt.annotate('Cleaning NaN titles ensures uniformity'" in line:
                # Use the exact same indentation as the line above it
                new_source.append("plt.annotate('Cleaning NaN titles ensures uniformity', xy=(0.5, 0.95), xycoords='axes fraction', ha='center', color='darkred')\n")
            else:
                new_source.append(line)
        cell['source'] = new_source
        break

with open(notebook_path, 'w', encoding='utf-8') as f:
    json.dump(nb, f, indent=1)

print("Indentation fixed.")
