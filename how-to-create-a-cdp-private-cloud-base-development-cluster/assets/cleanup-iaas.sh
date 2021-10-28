#!/bin/bash
#****************************************************************************
# (C) Cloudera, Inc. 2020-2021
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
#  Source File Name: cleanup-iaas.sh
#
#  Description: Terminate the 4-node cluster (AWS EC2 instances) that was
#               used for the trial version of
#               Cloudera Data Platform (CDP) Private Cloud Base.
#
#  Author(s): George Rueda de Leon
#****************************************************************************



#------------------------------------------
#    VARIABLE DEFINITION
#------------------------------------------
OWNER=""                                # OPTIONAL: unique identifer to
                                        # differentiate owner of CDP instance
KEYPAIR="$HOME/.ssh/${OWNER}cdp-trial-key.pem"
RETRY=25



#------------------------------------------
#     REMOVE EC2 INSTANCES
#------------------------------------------
printf "Removing instances"
EC2INSTANCES=($(aws ec2 describe-instances --filters "Name=key-name, Values=${OWNER}cdp-trial-key" "Name=instance-state-name, Values=running" | jq  -r '.Reservations[].Instances[] | .InstanceId'))
if [ ${#EC2INSTANCES[@]} -gt 0 ]; then
      aws ec2 terminate-instances --instance-ids ${EC2INSTANCES[@]} >/dev/null
      for instance in "${EC2INSTANCES[@]}"; do
            count=0
            complete=false
            while [ "$count" -lt "$RETRY" ] &&  [ "$complete" = "false" ]; do
                  MYSTATE=$(aws ec2 describe-instances --instance-ids ${instance} | jq  -r '.Reservations[].Instances[].State.Name')
                  if [ "$MYSTATE" != "terminated" ]; then
                        printf " ."
                        count=$(( count + 1 ))
                        sleep 5
                  else
                        complete=true
                  fi
            done

            if [ "$count" -ge "$RETRY" ] || [ "$complete" = "false" ]; then
                  printf " [FAILED to remove ${instance}]\n"
            fi
      done
fi
printf "\n"



#------------------------------------------
#     REMOVE SECURITY GROUP
#------------------------------------------
printf "Removing security group"
MYSECURITYGROUP=($(aws ec2 describe-security-groups --filters "Name=group-name, Values=${OWNER}cdp-trial-security-group" | jq  -r '.SecurityGroups[] | .GroupName'))
if [ ${#MYSECURITYGROUP[@]} -gt 0 ]; then
      aws ec2 delete-security-group --group-name ${OWNER}'cdp-trial-security-group'
fi
printf "\n"



#------------------------------------------
#     REMOVE KEY PAIR
#------------------------------------------
printf "Removing key-pair"
aws ec2 delete-key-pair --key-name ${OWNER}'cdp-trial-key'
if [ -f "$KEYPAIR" ]; then
      rm -f $KEYPAIR
fi
printf "\n"
