import urllib.request
import zipfile
import os
import shutil

# Download the fonts
url = 'https://downloads.sourceforge.net/project/dejavu/dejavu/2.37/dejavu-fonts-ttf-2.37.zip'
zip_path = os.path.expandvars(r'%TEMP%\dejavu-fonts.zip')
extract_path = os.path.expandvars(r'%TEMP%\dejavu-extract')

print('Downloading DejaVu fonts...')
urllib.request.urlretrieve(url, zip_path)

print('Extracting fonts...')
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)

# Copy the fonts
fonts_dir = os.path.dirname(__file__) + r'\fonts'
src_dir = os.path.join(extract_path, 'dejavu-fonts-ttf-2.37', 'ttf')

# Create fonts directory if it doesn't exist
os.makedirs(fonts_dir, exist_ok=True)

print('Copying fonts...')
for font_file in ['DejaVuSans.ttf', 'DejaVuSans-Bold.ttf']:
    src = os.path.join(src_dir, font_file)
    dst = os.path.join(fonts_dir, font_file)
    if os.path.exists(src):
        shutil.copy2(src, dst)
        print(f'Copied {font_file}')

# Cleanup
os.remove(zip_path)
shutil.rmtree(extract_path)
print('Done!')
