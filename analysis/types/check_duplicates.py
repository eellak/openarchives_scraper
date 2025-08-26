#!/usr/bin/env python3
"""
Check for duplicates and similar entries in type_rules.txt
"""
import re
from collections import defaultdict
from difflib import SequenceMatcher
import unicodedata


def remove_accents(text):
    """Remove accents from Greek text for comparison"""
    return ''.join(c for c in unicodedata.normalize('NFD', text)
                   if unicodedata.category(c) != 'Mn')


def similarity(a, b):
    """Calculate similarity between two strings"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def levenshtein_distance(s1, s2):
    """Calculate Levenshtein distance between two strings"""
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    
    if len(s2) == 0:
        return len(s1)
    
    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    
    return previous_row[-1]


def parse_rules(filename):
    """Parse the type rules file"""
    rules = []
    with open(filename, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            
            # Handle [[X]] marker
            has_marker = line.startswith('[[X]]')
            if has_marker:
                line = line[5:].strip()
            
            # Parse rule: token -> target : ["list", "of", "values"]
            match = re.match(r'^([^-]+?)\s*->\s*([^:]+?)\s*:\s*\[(.+)\]$', line)
            if match:
                token = match.group(1).strip()
                target = match.group(2).strip()
                values = match.group(3)
                rules.append({
                    'line': line_num,
                    'token': token,
                    'target': target,
                    'values': values,
                    'has_marker': has_marker,
                    'original_line': line
                })
    return rules


def check_duplicates(rules):
    """Check for various types of duplicates and similarities"""
    print("=== DUPLICATE CHECK REPORT ===\n")
    
    # 1. Check for exact duplicate tokens
    print("1. EXACT DUPLICATE TOKENS:")
    token_lines = defaultdict(list)
    for rule in rules:
        token_lines[rule['token']].append(rule['line'])
    
    found_dups = False
    for token, lines in token_lines.items():
        if len(lines) > 1:
            print(f"   Token '{token}' appears on lines: {', '.join(map(str, lines))}")
            found_dups = True
    if not found_dups:
        print("   No exact duplicate tokens found.")
    
    # 2. Check for exact duplicate targets
    print("\n2. EXACT DUPLICATE TARGETS:")
    target_info = defaultdict(list)
    for rule in rules:
        target_info[rule['target']].append((rule['line'], rule['token']))
    
    found_dups = False
    for target, info in target_info.items():
        if len(info) > 1:
            print(f"   Target '{target}' appears on:")
            for line, token in info:
                print(f"      Line {line}: {token} -> {target}")
            found_dups = True
    if not found_dups:
        print("   No exact duplicate targets found.")
    
    # 3. Check for similar tokens (Levenshtein distance ≤ 2)
    print("\n3. SIMILAR TOKENS (edit distance ≤ 2):")
    found_similar = False
    for i, rule1 in enumerate(rules):
        for rule2 in rules[i+1:]:
            dist = levenshtein_distance(rule1['token'], rule2['token'])
            if 0 < dist <= 2:
                print(f"   '{rule1['token']}' (line {rule1['line']}) ~ '{rule2['token']}' (line {rule2['line']}) [distance: {dist}]")
                found_similar = True
    if not found_similar:
        print("   No similar tokens found.")
    
    # 4. Check for similar targets (ignoring case and accents)
    print("\n4. SIMILAR TARGETS (ignoring case/accents):")
    found_similar = False
    for i, rule1 in enumerate(rules):
        for rule2 in rules[i+1:]:
            # Compare without accents and case
            target1_norm = remove_accents(rule1['target'].lower())
            target2_norm = remove_accents(rule2['target'].lower())
            
            if target1_norm == target2_norm and rule1['target'] != rule2['target']:
                print(f"   '{rule1['target']}' (line {rule1['line']}) ~ '{rule2['target']}' (line {rule2['line']})")
                found_similar = True
    if not found_similar:
        print("   No similar targets found.")
    
    # 5. Statistics
    print("\n5. STATISTICS:")
    print(f"   Total rules: {len(rules)}")
    print(f"   Unique tokens: {len(set(r['token'] for r in rules))}")
    print(f"   Unique targets: {len(set(r['target'] for r in rules))}")
    print(f"   Rules with [[X]] marker: {sum(1 for r in rules if r['has_marker'])}")
    
    # 6. Token/Target length analysis
    print("\n6. TOKEN LENGTH ANALYSIS:")
    token_lengths = defaultdict(int)
    for rule in rules:
        token_lengths[len(rule['token'])] += 1
    
    print("   Token length distribution:")
    for length in sorted(token_lengths.keys()):
        print(f"      Length {length}: {token_lengths[length]} tokens")
    
    # 7. Potentially problematic entries
    print("\n7. POTENTIALLY PROBLEMATIC ENTRIES:")
    print("   Tokens that might be too short or fragments:")
    for rule in rules:
        if len(rule['token']) <= 2 and not rule['has_marker']:
            print(f"      Line {rule['line']}: '{rule['token']}' -> '{rule['target']}'")
    
    print("\n   Tokens that don't match their targets well:")
    for rule in rules:
        if not rule['has_marker']:
            # Check if token is a substring of target or vice versa
            token_lower = rule['token'].lower()
            target_lower = rule['target'].lower()
            
            if (token_lower not in target_lower and 
                target_lower not in token_lower and
                similarity(rule['token'], rule['target']) < 0.5):
                print(f"      Line {rule['line']}: '{rule['token']}' -> '{rule['target']}'")


if __name__ == "__main__":
    rules = parse_rules("type_rules.txt")
    check_duplicates(rules)