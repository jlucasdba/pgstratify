#!/bin/bash

# define these to cross-build for another arch/os
#TARGETARCH=
#TARGETOS=

# source all the go environment variables
. <(go env)
if [[ ! -z $TARGETARCH ]]; then
	export GOARCH=${TARGETARCH}
fi
if [[ ! -z $TARGETOS ]]; then
	export GOOS=${TARGETOS}
fi
TMPDIR=$(mktemp -d)
VERSION=$(./pgstratify --version | awk '{ print $2 }')
RELNAME="pgstratify-${VERSION}-${GOOS}-${GOARCH}"
RELDIR="${TMPDIR}/${RELNAME}"
mkdir ${RELDIR}
go build -o ${RELDIR}/
cp -r examples ${RELDIR}
cp -r README.md ${RELDIR}
cp LICENSE ${RELDIR}
go-licenses save --save_path ${RELDIR}/licenses github.com/jlucasdba/pgstratify
OLDDIR="$PWD"
cd ${TMPDIR}
tar cvzf "${RELNAME}.tar.gz" ${RELNAME}
cd "${OLDDIR}"
cp "${TMPDIR}/${RELNAME}.tar.gz" .
rm -rf ${TMPDIR}
