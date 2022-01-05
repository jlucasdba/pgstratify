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
VERSION=$(git tag -l --points-at HEAD | sed 's/v//')
if [[ -z $VERSION ]]; then
	VERSION="git$(git log -1 --pretty=reference | awk '{print $1}')"
fi
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
