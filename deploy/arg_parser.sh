#!/bin/bash

POSITIONAL_ARGS=()

function help() {
  echo "Usage: `basename $0` [options] $ARG_PROMPT"
  echo "Options:"
  echo "  -h, --help                          Show this help message and exit"
  echo "  -v, --verbose                       Display verbose output"
  echo "  --code                              Update lambda code"
  echo "  --prefix=PREFIX                     Prefix for deployment names"
  echo "  --from=FROM                         Start deployment number"
  echo "  --to=TO                             End deployment number"
  echo "  -m, --mem=MEM                       Memory size in MB"
  echo "  --no-break                          Do not break for prompt"
  echo "  --no-build                          Skip building lambda code"
  echo "  --no-vpc                            Lambdas are not deployed in a VPC"
  echo "  --s3=BUCKET                         S3 bucket name"
}

if [ $# -eq 0 ] && [ $EXPECTING_ARGS ] ; then
  help
  exit 1
fi

while [[ $# -gt 0 ]]; do
  case $1 in
    -h|--help)
      help
      exit 0
      shift # past argument
      ;;
    -v|--verbose)
      VERBOSE=1
      shift # past argument
      ;;
    -code|--code)
      CODE="-code"
      shift # past argument
      ;;
    --prefix)
      DEPLOY_PREFIX="$2"
      shift # past argument
      shift # past value
      ;;
    --prefix=*)
      DEPLOY_PREFIX="${1#*=}"
      shift # past argument
      ;;
    --from)
      DEPLOY_FROM="$2"
      shift # past argument
      shift # past value
      ;;
    --from=*)
      DEPLOY_FROM="${1#*=}"
      shift # past argument
      ;;
    --to)
      DEPLOY_TO="$2"
      shift # past argument
      shift # past value
      ;;
    --to=*)
      DEPLOY_TO="${1#*=}"
      shift # past argument
      ;;
    -m|--mem)
      DEPLOY_MEM="$2"
      shift # past argument
      shift # past value
      ;;
    -m=*|--mem=*)
      DEPLOY_MEM="${1#*=}"
      shift # past argument
      ;;
    --no-break)
      NO_BREAK=1
      shift # past argument
      ;;
    --no-build)
      NO_BUILD=1
      shift # past argument
      ;;
    --no-vpc)
      VPC=
      shift # past argument
      ;;
    --s3)
      S3="$2"
      shift # past argument
      shift # past value
      ;;
    --s3=*)
      S3="${1#*=}"
      shift # past argument
      ;;
    -*|--*)
      echo "Unknown option $1"
      exit 1
      ;;
    *)
      POSITIONAL_ARGS+=("$1") # save positional arg
      shift # past argument
      ;;
  esac
done

set -- "${POSITIONAL_ARGS[@]}" # restore positional parameters
