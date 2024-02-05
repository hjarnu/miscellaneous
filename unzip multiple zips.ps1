# Set the path to the directory containing the zip files
$sourceDir = "C:\path\to\the\zip files"

# Set the path to the destination folder
$destinationDir = "X:\SteamLibrary\steamapps\common\Warhammer 40,000 DARKTIDE\mods"

# Create the destination folder if it doesn't exist
New-Item -ItemType Directory -Force -Path $destinationDir

# Get all zip files in the source directory
$zipFiles = Get-ChildItem -Path $sourceDir -Filter *.zip

# Loop through each zip file and extract its contents to the destination folder
foreach ($zipFile in $zipFiles) {
    $zipFileName = $zipFile.FullName
    Write-Host "Extracting $zipFileName..."
    Expand-Archive -Path $zipFileName -DestinationPath $destinationDir -Force
}

Write-Host "Extraction completed!"
