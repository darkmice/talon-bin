#!/usr/bin/env python3
"""Producer/CI only: materialize exact private build inputs. Never used by SDK consumers."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import tarfile
import tempfile

ROOT=Path(__file__).resolve().parent

def digest(data):return hashlib.sha256(data).hexdigest()
def tree(root):
    hasher=hashlib.sha256()
    for path in sorted(root.rglob('*')):
        if path.is_symlink():raise ValueError('Linked producer source is forbidden')
        if path.is_file():hasher.update(str(path.relative_to(root)).encode()+b'\0'+path.read_bytes()+b'\0')
    return hasher.hexdigest()

def verify(destination,package,manifest):
    if not destination.is_dir() or destination.is_symlink():raise ValueError('Invalid producer source directory')
    if tree(destination/'src')!=package['srcSha256']:raise ValueError('Producer source tree mismatch: '+package['name'])
    if digest((destination/'Cargo.toml').read_bytes())!=package['manifestSha256']:raise ValueError('Producer manifest mismatch: '+package['name'])
    if digest(manifest)!=package['manifestSha256']:raise ValueError('Tracked producer manifest mismatch')

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--offline',action='store_true');args=parser.parse_args()
    lock=json.loads((ROOT/'native/sources.lock.json').read_bytes());base=ROOT/'native/.sources';base.mkdir(exist_ok=True)
    for package in lock['packages']:
        name=package['name']
        if package['sourceMode']!='git_commit' or not re.fullmatch(r'[0-9a-f]{40}',package['commit']):raise ValueError('Unpublished source identity')
        if name not in {'talon-agent','talon-ai','talon-llm'} or package['repository']!='darkmice/'+name:raise ValueError('Unexpected private input repository')
        manifest=(ROOT/'native/manifests'/f'{name}.toml').read_bytes()
        if digest(manifest)!=package['manifestSha256']:raise ValueError('Tracked producer manifest mismatch')
        destination=base/name
        if destination.exists() or destination.is_symlink():verify(destination,package,manifest);continue
        if args.offline:raise ValueError('Producer cache is missing: '+name)
        with tempfile.TemporaryDirectory(prefix='.source-',dir=base) as temporary:
            temporary=Path(temporary);archive=temporary/'source.tar.gz';stage=temporary/'package';stage.mkdir()
            with archive.open('xb') as output:subprocess.run(['gh','api',f"repos/{package['repository']}/tarball/{package['commit']}"],stdout=output,check=True,timeout=180)
            if archive.stat().st_size>256*1024*1024:raise ValueError('Producer source archive exceeds limit')
            seen=set();total=0
            with tarfile.open(archive,'r:gz') as source:
                for member in source:
                    parts=PurePosixPath(member.name).parts
                    if len(parts)<2:continue
                    relative=PurePosixPath(*parts[1:])
                    if relative.parts[0]!='src' and str(relative) not in {'README.md','LICENSE','LICENSE.md','NOTICE'}:continue
                    if '..' in relative.parts or relative.is_absolute():raise ValueError('Unsafe source path')
                    if member.isdir():continue
                    if not member.isfile() or str(relative) in seen or member.size>32*1024*1024:raise ValueError('Unsafe source member')
                    seen.add(str(relative));total+=member.size
                    if len(seen)>4096 or total>128*1024*1024:raise ValueError('Producer source tree exceeds limit')
                    path=stage/str(relative);path.parent.mkdir(parents=True,exist_ok=True)
                    with source.extractfile(member) as incoming,path.open('xb') as output:shutil.copyfileobj(incoming,output)
            (stage/'Cargo.toml').write_bytes(manifest)
            verify(stage,package,manifest)
            if destination.exists() or destination.is_symlink():raise ValueError('Producer source appeared concurrently')
            stage.rename(destination)
        print(f"Verified producer input {name}@{package['commit']}")

if __name__=='__main__':main()
