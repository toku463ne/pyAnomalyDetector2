"""
This script renders a Jinja2 template with variables  and saves the output to a file.
"""
import os
import sys
import jinja2
from pathlib import Path
from dotenv import load_dotenv
# Load environment variables from .env file
load_dotenv()

src_file = sys.argv[1]
dst_file = sys.argv[2]


# read the template file
template_file = Path(src_file)
if not template_file.is_file():
    print(f"Template file {template_file} does not exist.")
    sys.exit(1)

# render the template
env = jinja2.Environment(
    loader=jinja2.FileSystemLoader(template_file.parent),
    autoescape=jinja2.select_autoescape(['html', 'xml'])
)
template = env.get_template(template_file.name)

rendered = template.render(os.environ)
# save the rendered template to a file
dst_file = Path(dst_file)
if not dst_file.parent.is_dir():
    print(f"Destination directory {dst_file.parent} does not exist.")
    sys.exit(1)
with open(dst_file, 'w') as f:
    f.write(rendered)
print(f"Rendered template saved to {dst_file}")
