#!/usr/bin/env python3
"""Package and sign a clean native runtime after native and SDK acceptance."""
import argparse
import ctypes
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile

ROOT=Path(__file__).resolve().parent
KEY_ID='talon-enterprise-core-2026-10-01'
KEY_SHA='617eca89ae8b1e824b5ae764cee171f98eff07e16123e7a0dbead73425b1fa2c'
def sha(data):return hashlib.sha256(data).hexdigest()
def run(*args):return subprocess.check_output(args)
def require(condition,message):
    if not condition:raise ValueError(message)
def identity(library):
    lib=ctypes.CDLL(str(library));lib.talon_runtime_call_v1.argtypes=[ctypes.c_void_p,ctypes.c_size_t,ctypes.POINTER(ctypes.c_void_p)];lib.talon_runtime_call_v1.restype=ctypes.c_int;lib.talon_runtime_free_v1.argtypes=[ctypes.c_void_p];lib.talon_runtime_free_v1.restype=None
    request=b'{"action":"identity"}';out=ctypes.c_void_p()
    try:
        require(lib.talon_runtime_call_v1(request,len(request),ctypes.byref(out))==0 and out.value,'Native identity failed')
        envelope=json.loads(ctypes.string_at(out));require(envelope['ok'] is True,'Native identity rejected');return envelope['data']
    finally:
        if out.value:lib.talon_runtime_free_v1(out)
def record(path):return {'path':path.name,'size':path.stat().st_size,'sha256':sha(path.read_bytes())}

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ['library','output','private-key','public-key','evidence','metadata']:parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--target',choices=['macos-arm64','macos-amd64','linux-arm64','linux-amd64'],required=True)
    args=parser.parse_args();repo=ROOT.parent;commit=run('git','-C',str(repo),'rev-parse','HEAD').decode().strip()
    require(not run('git','-C',str(repo),'status','--porcelain').strip(),'Producer tree must be clean')
    native=identity(args.library.resolve());sources=json.loads((ROOT/'native/sources.lock.json').read_bytes())
    require(native['producerCommit']==commit and native['producerDirty'] is False,'Binary producer attestation mismatch')
    require(native['protocolVersion']==1 and native['profile']=='platform-host-v1','Native protocol mismatch')
    require(native['sources']==sources and all(p['sourceMode']=='git_commit' for p in sources['packages']),'Source attestation mismatch')
    require(native['core']['git_commit']==sources['coreCommit'] and native['core']['git_dirty'] is False,'Core attestation mismatch')
    require(re.fullmatch(r'\d+\.\d+\.\d+',native['runtimeVersion']) is not None,'Invalid runtime version')
    tag='runtime-v'+native['runtimeVersion'];epoch=int(run('git','-C',str(repo),'show','-s','--format=%ct','HEAD'))
    triples={'macos-arm64':'aarch64-apple-darwin','macos-amd64':'x86_64-apple-darwin','linux-arm64':'aarch64-unknown-linux-gnu','linux-amd64':'x86_64-unknown-linux-gnu'}
    require(native['core']['target']==triples[args.target],'Native target mismatch')
    key_der=run('openssl','pkey','-pubin','-in',str(args.public_key),'-outform','DER');require(sha(key_der)==KEY_SHA,'Signing key fingerprint mismatch')
    evidence={}
    for name in ['native-tests.log','sdk-profile.log']:
        path=args.evidence/name;require(path.is_file(),'Missing acceptance evidence: '+name)
        log=path.read_text();require('test result: ok.' in log and 'test result: FAILED' not in log and 'error: could not compile' not in log,'Failed acceptance evidence: '+name)
        if name=='native-tests.log':require('3 passed; 0 failed' in log,'Incomplete native ABI coverage')
        else:
            for test in ['admitted_binary_preserves_shared_store_callbacks_stream_and_cancel','real_native_provider_preserves_stream_transport_http_errors_and_file_policy']:require(test+' ... ok' in log,'Incomplete SDK native coverage')
        evidence[name]=record(path)
    public_consumer=args.evidence/'public-consumer.json';consumer=json.loads(public_consumer.read_bytes());names={package['name'] for package in consumer['packages']}
    require({'talon-core-sdk','talon-llm-sdk','talon-agent-sdk','talon-runtime-transport'}<=names and not names&{'talon','talon-agent','talon-ai','talon-llm','talon-session','talon-block'},'Consumer still resolves private implementation sources')
    evidence['public-consumer.json']=record(public_consumer)
    args.output.mkdir(parents=True,exist_ok=False)
    prefix='talon-runtime-'+args.target;archive_path=args.output/(prefix+'.tar.gz')
    metadata=json.loads(args.metadata.read_bytes());components=[]
    private={package['name']:package for package in sources['packages']}
    for package in metadata['packages']:
        origin=package.get('source')
        if package['name']=='talon':origin='git+https://github.com/darkmice/talon-core#'+sources['coreCommit']
        if package['name'] in private:origin='git+https://github.com/'+private[package['name']]['repository']+'#'+private[package['name']]['commit']
        if origin is None:origin='git+https://github.com/darkmice/talon-bin#'+commit
        require('file:' not in origin and '@github.com' not in origin,'Unsafe metadata origin')
        components.append({'type':'library','name':package['name'],'version':package['version'],'purl':f"pkg:cargo/{package['name']}@{package['version']}",'properties':[{'name':'talon:source','value':origin},{'name':'talon:declared-license','value':package.get('license') or 'See upstream license file'}]})
    sbom={'bomFormat':'CycloneDX','specVersion':'1.5','version':1,'metadata':{'component':{'type':'library','name':'talon-native-runtime','version':native['runtimeVersion']},'properties':[{'name':'talon:scope','value':'resolved native build and test dependency graph'}]},'components':sorted(components,key=lambda item:(item['name'],item['version']))}
    sbom_path=args.output/(prefix+'.sbom.cdx.json');sbom_path.write_text(json.dumps(sbom,sort_keys=True,separators=(',',':'))+'\n')
    inventory=args.output/(prefix+'.licenses.json');inventory.write_text(json.dumps([{'name':item['name'],'version':item['version'],'declaredLicense':item['properties'][1]['value']} for item in sbom['components']],sort_keys=True,separators=(',',':'))+'\n')
    acceptance=args.output/(prefix+'.acceptance.json');acceptance.write_text(json.dumps({'producerCommit':commit,'target':args.target,'nativeAbiTests':3,'sdkIntegrationTests':2,'publicOnlyConsumer':True,'workflowRunId':os.environ.get('GITHUB_RUN_ID'),'logs':evidence},sort_keys=True,separators=(',',':'))+'\n')
    library_name='libtalon_runtime.dylib' if args.target.startswith('macos') else 'libtalon_runtime.so'
    entries={library_name:args.library.resolve(),'talon_runtime.h':ROOT/'native/include/talon_runtime.h','sources.lock.json':ROOT/'native/sources.lock.json','LICENSE':repo/'LICENSE','NOTICE':repo/'NOTICE',sbom_path.name:sbom_path,inventory.name:inventory,acceptance.name:acceptance}
    members=[]
    with archive_path.open('xb') as file,gzip.GzipFile(filename='',mode='wb',fileobj=file,mtime=epoch) as compressed,tarfile.open(fileobj=compressed,mode='w') as archive:
        for name,path in sorted(entries.items()):
            content=path.read_bytes();info=tarfile.TarInfo(name);info.size=len(content);info.mode=0o644;info.mtime=epoch;info.uid=info.gid=0;archive.addfile(info,io.BytesIO(content));members.append({'path':name,'size':len(content),'sha256':sha(content)})
    manifest={'format':1,'releaseTag':tag,'producerCommit':commit,'profile':'platform-host-v1','protocolVersion':1,'target':args.target,'nativeIdentity':native,'archive':record(archive_path),'members':members,'build':{'rustc':run('rustc','--version').decode().strip(),'cargoLockSha256':sha((ROOT/'native/Cargo.lock').read_bytes()),'sdkLockSha256':sha((ROOT/'sdk/Cargo.lock').read_bytes()),'workflowRunId':os.environ.get('GITHUB_RUN_ID'),'evidence':evidence},'signing':{'keyId':KEY_ID,'publicKeyDerSha256':KEY_SHA}}
    manifest_path=args.output/(prefix+'.manifest.json');manifest_path.write_text(json.dumps(manifest,sort_keys=True,separators=(',',':'))+'\n')
    signature=args.output/(manifest_path.name+'.sig');run('openssl','pkeyutl','-sign','-rawin','-inkey',str(args.private_key),'-in',str(manifest_path),'-out',str(signature))
    run('openssl','pkeyutl','-verify','-rawin','-pubin','-inkey',str(args.public_key),'-in',str(manifest_path),'-sigfile',str(signature))
    (args.output/'talon-runtime-public.pem').write_bytes(args.public_key.read_bytes())
    print(json.dumps({'releaseTag':tag,'producerCommit':commit,'target':args.target,'archive':record(archive_path),'manifest':record(manifest_path)}))

if __name__=='__main__':main()
