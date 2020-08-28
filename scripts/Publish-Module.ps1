poetry build
$src = Get-ChildItem "$(git root)/dist/*.whl" |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1
dbfs cp --overwrite $src "dbfs:/lib/$($src.Name)"
