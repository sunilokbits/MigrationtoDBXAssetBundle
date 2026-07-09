"""Remove all AI references from templates and bom/help."""
import os

BASE = r'C:\Live_MigrationProject\Databrciks_Poc\Poc\MigrationtoDBXAssetBundle'

def fix_file(relpath, replacements):
    path = os.path.join(BASE, relpath)
    with open(path, 'r', encoding='utf-8') as f:
        c = f.read()
    changed = False
    for old, new in replacements:
        if old in c:
            cnt = c.count(old)
            c = c.replace(old, new)
            print(f"  [{relpath}] '{old[:50]}' -> {cnt}x")
            changed = True
    if changed:
        with open(path, 'w', encoding='utf-8') as f:
            f.write(c)
        print(f"  SAVED: {relpath}")
    else:
        print(f"  NO CHANGES: {relpath}")

# index.html
fix_file('migration_utility/templates/index.html', [
    ('AI Workflow Manager', 'Workflow Manager'),
    ('AI WORKFLOW MANAGER', 'WORKFLOW MANAGER'),
])

# bom.html
fix_file('migration_utility/templates/bom.html', [
    ('AI-powered, browser-based platform', 'browser-based platform'),
    ('AI Convert to PySpark', 'Convert to PySpark'),
    ('One-click AI conversion of T-SQL', 'One-click conversion of T-SQL'),
    ('AI-powered error diagnosis', 'Automated error diagnosis'),
    ('AI-driven Star/Snowflake classification', 'Star/Snowflake classification'),
    ('AI-Powered Conversion Engine', 'Automated Conversion Engine'),
    ('Uses advanced AI to understand T-SQL semantics', 'Understands T-SQL semantics'),
    ('AI classifies tables as Facts or Dimensions', 'Classifies tables as Facts or Dimensions'),
    ('AI-powered error analysis understands', 'Automated error analysis understands'),
    ('AI-automated', 'Automated'),
    ('Instant AI diagnosis', 'Instant diagnosis'),
    ('AI-assisted conversion', 'Automated conversion'),
    ('AI converts T-SQL to PySpark notebooks', 'Converts T-SQL to PySpark notebooks'),
    ('AI classifies Star/Snowflake schemas', 'Classifies Star/Snowflake schemas'),
    ('<td><strong>AI Engine</strong></td>', '<td><strong>Conversion Engine</strong></td>'),
    ('AI Conversion', 'Conversion Engine'),
    ('with AI handling 90%', 'handling 90%'),
    ('AI-Automated Conversion', 'Automated Conversion'),
    ('using AI (OpenAI / Azure OpenAI)', 'using OpenAI / Azure OpenAI'),
    ('using AI-driven Star/Snowflake', 'using Star/Snowflake'),
    ('<li><strong>AI:</strong> OpenAI GPT', '<li><strong>Engine:</strong> OpenAI GPT'),
])

# help.html
fix_file('migration_utility/templates/help.html', [
    ('AI-powered transformation', 'automated transformation'),
    ('AI Conversion', 'Conversion'),
    ('AI-driven data modeling engine', 'Data modeling engine'),
    ('AI classifies tables as Facts/Dimensions', 'Classifies tables as Facts/Dimensions'),
    ('(AI decides)', '(system decides)'),
    ('AI classifies tables as Fact or Dimension', 'Classifies tables as Fact or Dimension'),
    ('AI Classification', 'Auto Classification'),
    ('AI-powered diagnosis', 'automated diagnosis'),
    ('get AI-powered', 'get automated'),
    ('AI diagnosis', 'automated diagnosis'),
    ('the AI analysis engine', 'the analysis engine'),
    ('AI-powered', 'Automated'),
])

print("\nDone! All AI references removed.")
