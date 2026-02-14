import json
import re
import os

def clean_notebook(p):
    try:
        with open(p, 'r', encoding='utf-8') as f:
            d = json.load(f)
    except:
        return
        
    modified = False
    for cell in d.get('cells', []):
        if cell.get('cell_type') == 'markdown':
            new_source = []
            source = cell.get('source', [])
            if isinstance(source, str):
                source = [source]
                
            for line in source:
                if line.lstrip().startswith('#'):
                    # Strip non-ASCII (emojis)
                    new_line = re.sub(r'[^\x00-\x7F]+', '', line)
                    # Clean double spaces
                    new_line = new_line.replace('  ', ' ').strip()
                    # Ensure space after #
                    new_line = re.sub(r'^(#+)([^# ])', r'\1 \2', new_line)
                    
                    if line.endswith('\n') and not new_line.endswith('\n'):
                        new_line += '\n'
                    
                    if new_line != line:
                        modified = True
                        line = new_line
                new_source.append(line)
            cell['source'] = new_source
            
    if modified:
        with open(p, 'w', encoding='utf-8') as f:
            json.dump(d, f, indent=1, ensure_ascii=False)
        print(f"Cleaned: {p}")

for root, dirs, files in os.walk('examples'):
    for f in files:
        if f.endswith('.ipynb'):
            clean_notebook(os.path.join(root, f))
