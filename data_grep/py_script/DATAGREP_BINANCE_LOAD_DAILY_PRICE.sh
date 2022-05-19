#!/bin/bash
#------------------------------------------------------------------
# Copyright (c) Passion Basketball Team All Rights Reserved
#
# PROJECT NAME    : Stock
#
# PROGRAM NAME    : CRYPTO_GREP_TIDEBIT_USDTHKD_TG.sh
#
# PURPOSE         : 
#
# INPUT           : Nil
#
# OUTPUT          : Nil
#
# SCRIPT INCLUDED : Nil
#
# STORE PROCEDURE
# INCLUDED        : Nil
#
#------------------------------------------------------------------
#
# REF NO          : NIL
# DATE MODIFIED   : 5-Jan-2020
# MODIFIED BY     : Zola Wong
# Reference Web   : 
#
#------------------------------------------------------------------
#
# CHANGES         :
#   REF NO   DATE      WHO                   DETAIL
#   ------   --------- --------------------- ------------------------
#
#------------------------------------------------------------------
. $HOME/Dropbox/production/.profile
. $HOME/Dropbox/production/.conn_mysql

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Declare and initialize variables
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
PROG_NAME=`basename $0`
PY_PATH="$HOME/Dropbox/prj/py/crypto/data_grep"
DB_NAME='crypto'
JOB_TYPE="SCRIPT"
PROCESS_NAME="DATAGREP_BINANCE_LOAD_DAILY_PRICE"

function check_before_start
{

STATUS=`mysql -u ${ROOT_USER} -p${ROOT_PW} $DB_NAME <<EOS! | sed -n 2p
select process_status from process_control 
where JOB_TYPE = '${JOB_TYPE}'
and  PROCESS_NAME = '${PROCESS_NAME}'
EOS!`

CT=`mysql -u ${ROOT_USER} -p${ROOT_PW} $DB_NAME <<EOS! | sed -n 2p
select fail_count from process_control 
where JOB_TYPE = '${JOB_TYPE}'
and  PROCESS_NAME = '${PROCESS_NAME}'
EOS!`

}


function check_after_exe ()
{

    if [[ $1 -ne 0 ]]; then
    
        CT=$((CT+1))
        update_process_status 'F' $CT
        echo "Please check" | mail -s "${PROCESS_NAME} Program fail to process" wongzola@gmail.com 
        exit 0
        
    else
    
        update_process_status 'C' 0
        exit 0
    
    fi
}

update_process_status()
{
PARA1=$1
PARA2=$2
mysql -u ${ROOT_USER} -p${ROOT_PW} $DB_NAME <<EOS!
update process_control 
set process_status = "${PARA1}", fail_count = ${PARA2}, update_dt = SYSDATE() 
where JOB_TYPE = "${JOB_TYPE}"
and  PROCESS_NAME = "${PROCESS_NAME}"
EOS!

}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Call java WaiGorCommentProgram.jar
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

check_before_start
if [[ $STATUS == 'C' ]]; then
    
    update_process_status 'P' $CT
    cd ${PY_PATH}
    python3  DATAGREP_BINANCE_LOAD_DAILY_PRICE.py
    check_after_exe $?

elif  [[ $STATUS == 'P' ]]; then

    echo "Please check" | mail -s "${PROCESS_NAME} Program still processing" wongzola@gmail.com 

elif  [[ $STATUS == 'F' ]]; then
    
    if [[ $CT -lt 3 ]]; then
        update_process_status 'P' $CT
        cd ${PY_PATH}
        python3  DATAGREP_BINANCE_LOAD_DAILY_PRICE.py
        check_after_exe $?
    else
        echo "Please check" | mail -s "${PROCESS_NAME} Program reach max fail count" wongzola@gmail.com    
    fi
    
fi