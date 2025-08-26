#!/usr/bin/env python3
"""Fix type rules according to analysis"""

import json
import re

def main():
    # Read all lines
    with open('type_rules.txt', 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # 1. First extract values from duplicate lines for merging
    # Line 128 (0-indexed: 127): αρθο -> άρθρο : ['άρθο']
    line_127 = lines[127].strip()
    match = re.search(r'\[(.*?)\]', line_127)
    value_127 = json.loads('[' + match.group(1) + ']')[0] if match else None
    
    # Line 138 (0-indexed: 137): ευχος -> τεύχος : ['εύχος']
    line_137 = lines[137].strip() 
    match = re.search(r'\[(.*?)\]', line_137)
    value_137 = json.loads('[' + match.group(1) + ']')[0] if match else None
    
    # Process each line
    new_lines = []
    for i, line in enumerate(lines):
        # NO DELETION - process all lines
            
        # Process the line
        line = line.rstrip('\n')
        
        # 2. Merge duplicates
        if i == 3 and 'αρθρο -> άρθρο' in line:  # Line 4
            match = re.search(r'\[(.*?)\]', line)
            if match and value_127:
                values = json.loads('[' + match.group(1) + ']')
                if value_127 not in values:
                    values.append(value_127)
                prefix = line[:line.index('[')]
                line = prefix + json.dumps(values, ensure_ascii=False)
                
        elif i == 34 and 'τευχος -> τεύχος' in line:  # Line 35
            match = re.search(r'\[(.*?)\]', line)
            if match and value_137:
                values = json.loads('[' + match.group(1) + ']')
                if value_137 not in values:
                    values.append(value_137)
                prefix = line[:line.index('[')]
                line = prefix + json.dumps(values, ensure_ascii=False)
        
        # 3. Update labels for [[X]] marked rules
        if line.startswith('[[X]] en -> EN'):
            line = line.replace('[[X]] en -> EN', 'en -> αριθμός/number')
        elif line.startswith('[[X]] el -> EL'):
            line = line.replace('[[X]] el -> EL', 'el -> αριθμός/number')
        elif line.startswith('[[X]] τυχιακ -> πτυχιακή'):
            line = line.replace('[[X]] τυχιακ -> πτυχιακή', 'τυχιακ -> μεταπτυχιακή/πτυχιακή')
        elif line.startswith('[[X]] αποφασ -> απόφαση'):
            line = line.replace('[[X]] αποφασ -> απόφαση', 'αποφασ -> δικαστική απόφαση')
        elif line.startswith('[[X]] cation -> publication'):
            line = line.replace('[[X]] cation -> publication', 'cation -> επιστημονική δημοσίευση')
        elif line.startswith('[[X]] ection -> collection'):
            line = line.replace('[[X]] ection -> collection', 'ection -> συλλογή/εκλογές')
        elif line.startswith('[[X]] antics -> semantics'):
            line = line.replace('[[X]] antics -> semantics', 'antics -> μεταδεδομένα/metadata')
            
        # 4. Add [[X]] markers and update labels for additional problematic rules
        elif line.startswith('erence -> conference'):
            line = line.replace('erence -> conference', '[[X]] erence -> συνέδριο/αναφορά')
        elif line.startswith('επιστη -> επιστημονικό'):
            line = line.replace('επιστη -> επιστημονικό', '[[X]] επιστη -> πανεπιστημιακό')
        elif line.startswith('mp -> mp3'):
            line = line.replace('mp -> mp3', '[[X]] mp -> media/πολυμέσα')
        elif line.startswith('αστηρι -> εργαστήριο'):
            line = '[[X]] ' + line
        elif line.startswith('αγραφε -> προδιαγραφές'):
            line = '[[X]] ' + line
        elif line.startswith('γχομεν -> ελεγχόμενο'):
            line = '[[X]] ' + line
            
        # 5. Mark entries that need special handling but keep them
        elif i == 119:  # Line 120: rape
            line = '[[X]] ' + line if not line.startswith('[[X]]') else line
        elif i == 123:  # Line 124: Ψειρίδου  
            line = '[[X]] ' + line if not line.startswith('[[X]]') else line
        elif i == 128:  # Line 129: 1751 document
            line = '[[X]] ' + line if not line.startswith('[[X]]') else line
        elif i == 127:  # Line 128: αρθο (duplicate)
            line = '[[D]] ' + line if not line.startswith('[[D]]') else line
        elif i == 137:  # Line 138: ευχος (duplicate)
            line = '[[D]] ' + line if not line.startswith('[[D]]') else line
            
        new_lines.append(line + '\n')
    
    # Write the updated file
    with open('type_rules_fixed.txt', 'w', encoding='utf-8') as f:
        f.writelines(new_lines)
        
    print(f"Original lines: {len(lines)}")
    print(f"New lines: {len(new_lines)}")
    print(f"Fixed rules written to type_rules_fixed.txt")
    print(f"\nMerged duplicates:")
    if value_127:
        print(f"  - Merged 'άρθο' from line 128 into line 4")
    if value_137:
        print(f"  - Merged 'εύχος' from line 138 into line 35")
    print(f"\nMarked problematic entries with [[X]] for review")

if __name__ == "__main__":
    main()