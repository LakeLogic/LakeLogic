import json
import os
import sys

def clean_notebook(p):
    try:
        with open(p, 'r', encoding='utf-8') as f:
            d = json.load(f)
    except:
        return
        
    modified = False
    for cell in d.get('cells', []):
        if cell.get('cell_type') == 'markdown':
            source = cell.get('source', [])
            if not isinstance(source, list):
                continue
                
            new_source = []
            for line in source:
                orig_line = line
                
                # Brute force ASCII cleaning for all markdown text
                # This removes all non-ASCII characters including emojis
                try:
                    line = line.encode('ascii', 'ignore').decode('ascii')
                except:
                    pass
                
                # Clean up spacing for headers
                if orig_line.lstrip().startswith('#'):
                    line = line.replace('  ', ' ').strip()
                    import re
                    line = re.sub(r'^(#+)([^# ])', r'\1 \2', line)
                    if orig_line.endswith('\n'):
                        line += '\n'
                
                if line != orig_line:
                    modified = True
                new_source.append(line)
            cell['source'] = new_source
            
    if modified:
        with open(p, 'w', encoding='utf-8') as f:
            json.dump(d, f, indent=1, ensure_ascii=False)
        print(f"CLEANED: {p}")
        sys.stdout.flush()

for root, dirs, files in os.walk('examples'):
    for f in files:
        if f.endswith('.ipynb'):
            clean_notebook(os.path.join(root, f))
print("FINISH_ALL")
sys.stdout.flush()
