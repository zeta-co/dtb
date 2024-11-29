import os
import shutil
import re
import yaml
from pathlib import Path

def extract_nav_order(nav_item):
    """Recursively extract file ordering from nav section of mkdocs.yml"""
    order_map = {}
    
    def process_item(item, parent_path=""):
        if isinstance(item, dict):
            for section_name, section_content in item.items():
                new_path = f"{parent_path}/{section_name}" if parent_path else section_name
                if isinstance(section_content, list):
                    order_map[new_path] = []
                    for sub_item in section_content:
                        process_item(sub_item, new_path)
                else:
                    # Store the file path without .md extension
                    file_path = section_content.replace(".md", "")
                    parent_dir = str(Path(file_path).parent)
                    if parent_dir not in order_map:
                        order_map[parent_dir] = []
                    order_map[parent_dir].append(Path(file_path).name)
        elif isinstance(item, list):
            for sub_item in item:
                process_item(sub_item, parent_path)
        elif isinstance(item, str):
            # Handle direct string entries
            pass

    for item in nav_item:
        process_item(item)
    
    return order_map

def convert_to_azure_wiki(docs_dir: str, wiki_output_dir: str, mkdocs_file: str):
    """
    Convert MkDocs markdown documentation to Azure DevOps Wiki format.
    
    Args:
        docs_dir (str): Directory containing source markdown files (docs folder)
        wiki_output_dir (str): Output directory for Azure DevOps Wiki
        mkdocs_file (str): Path to mkdocs.yml file
    """
    # Read mkdocs.yml to get the navigation order
    with open(mkdocs_file, 'r') as f:
        mkdocs_config = yaml.safe_load(f)
    
    # Extract order from nav section
    nav_order = extract_nav_order(mkdocs_config.get('nav', []))
    print("Extracted navigation order:", nav_order)

    def process_markdown_file(content: str) -> str:
        # Remove MkDocs-specific syntax
        content = re.sub(r'\{[^\}]+\}', '', content)
        
        # Fix internal links to match Azure Wiki format
        content = re.sub(r'\[([^\]]+)\]\((?!http)([^\)]+)\.md\)', r'[\1](\2)', content)
        
        # Remove mkdocstrings specific syntax
        content = re.sub(r'::: .*\n', '', content)
        content = re.sub(r'    options:\n.*\n.*\n.*\n', '', content)
        
        return content

    def create_order_file(directory: Path, files: list):
        """Create .order file for Azure Wiki directory"""
        rel_dir = str(directory.relative_to(wiki_dir))
        rel_dir = '.' if rel_dir == '' else rel_dir
        
        # Get the ordered list from nav_order if it exists
        ordered_files = nav_order.get(rel_dir, [])
        
        # Create set of all files in this directory
        available_files = {f.stem for f in files}
        if 'index' in available_files:
            available_files.remove('index')
            available_files.add('README')
        
        # Start with ordered files from mkdocs.yml
        final_order = []
        for name in ordered_files:
            if name == 'index':
                final_order.append('README')
            else:
                final_order.append(name)
        
        # Add any remaining files that weren't in the nav
        remaining_files = available_files - set(final_order)
        final_order.extend(sorted(remaining_files))
        
        if final_order:
            order_file = directory / '.order'
            order_file.write_text('\n'.join(final_order))
            print(f"Created .order file in {directory} with contents:\n{final_order}")

    # Create wiki directory structure
    wiki_dir = Path(wiki_output_dir)
    if wiki_dir.exists():
        shutil.rmtree(wiki_dir)
    wiki_dir.mkdir(parents=True)

    source_dir = Path(docs_dir)
    if not source_dir.exists():
        raise FileNotFoundError(f"Documentation directory not found: {source_dir}")

    # Keep track of files in each directory
    directory_contents = {}

    # Process all markdown files
    for src_path in source_dir.rglob('*.md'):
        try:
            rel_path = src_path.relative_to(source_dir)
            dest_path = wiki_dir / rel_path
            
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            
            if dest_path.parent not in directory_contents:
                directory_contents[dest_path.parent] = []
            directory_contents[dest_path.parent].append(src_path)
            
            content = src_path.read_text(encoding='utf-8')
            processed_content = process_markdown_file(content)
            
            if dest_path.name.lower() == 'index.md':
                dest_path = dest_path.parent / 'README.md'
            
            dest_path.write_text(processed_content, encoding='utf-8')
            print(f"Processed: {src_path} -> {dest_path}")
            
        except Exception as e:
            print(f"Error processing {src_path}: {str(e)}")

    # Copy images if they exist
    for src_path in source_dir.rglob('*'):
        if src_path.is_file() and src_path.suffix.lower() in ['.png', '.jpg', '.jpeg', '.gif']:
            try:
                rel_path = src_path.relative_to(source_dir)
                dest_path = wiki_dir / rel_path
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src_path, dest_path)
                print(f"Copied image: {src_path} -> {dest_path}")
            except Exception as e:
                print(f"Error copying {src_path}: {str(e)}")

    # Create .order files for each directory
    for directory, files in directory_contents.items():
        create_order_file(directory, files)

if __name__ == '__main__':
    # Convert from docs directory instead of site
    convert_to_azure_wiki('docs', 'wikidocs', 'mkdocs.yml')
