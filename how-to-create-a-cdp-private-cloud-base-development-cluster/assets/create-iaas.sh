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
#  Source File Name: create-iaas.sh
#
#  Description: Prepare a 4-node cluster (AWS EC2 instances) for the
#               purpose of installing the trial version of
#               Cloudera Data Platform (CDP) Private Cloud Base.
#
#  Documentation for CDP Private Cloud Base:
#      https://docs.cloudera.com/cdp-private-cloud-base/7.1.7/index.html
#
#  Trial Installation Instructions:
#      https://docs.cloudera.com/cdp-private-cloud-base/7.1.7/installation/topics/cdpdc-trial-installation.html
#
#  Author(s): George Rueda de Leon
#****************************************************************************



#------------------------------------------
#    VARIABLE DEFINITION
#------------------------------------------
OWNER=""                                # OPTIONAL: unique identifer to
                                        # differentiate owner of CDP instance
KEYPAIR="$HOME/.ssh/${OWNER}cdp-trial-key.pem"
NUM_NODES=4
RETRY=25
LOCAL_IP=""
VPCID=""
GROUPID=""
SUBNET=""



#------------------------------------------
#     GET YOUR IP ADDRESS
#------------------------------------------
printf "Getting your ip address"
LOCAL_IP="$(curl -s https://checkip.amazonaws.com)"
printf "\n"



#------------------------------------------
#     CREATE KEY PAIR
#------------------------------------------
printf "Creating key-pair"
mkdir -p $HOME/.ssh
if [ -f "$KEYPAIR" ]; then
      printf "\n\tWARNING: key-pair alreadys exists"
else
      aws ec2 create-key-pair                               \
            --key-name ${OWNER}'cdp-trial-key'              \
            --query 'KeyMaterial'                           \
            --output text > $KEYPAIR && \
            chmod 400 $KEYPAIR
fi
printf "\n"



#------------------------------------------
#     CREATE SECURITY GROUP
#------------------------------------------
printf "Creating security group"
MYSECURITYGROUP=($(aws ec2 describe-security-groups --filters "Name=group-name, Values=${OWNER}cdp-trial-security-group" | jq  -r '.SecurityGroups[] | "\(.GroupId),\(.VpcId)"'))
if [ ${#MYSECURITYGROUP[@]} -gt 0 ]; then
      printf "\n\tWARNING: Security Group already exists"
      GROUPID=$(echo "${MYSECURITYGROUP}" | awk '{split($0,a,","); print a[1]}')
      VPCID=$(echo "${MYSECURITYGROUP}" | awk '{split($0,a,","); print a[2]}')
else
      MYVPCS="$(aws ec2 describe-vpcs                             \
            --filters "Name=is-default,Values=true"               \
            "Name=cidr-block-association.state, Values=associated"\
            "Name=state,Values=available"                         \
            | jq -r '.Vpcs[] | "\(.VpcId),\(.CidrBlock)"')"

      VPCID=$(echo "${MYVPCS}" | awk '{split($0,a,","); print a[1]}')
      CIDRBLOCK=$(echo "${MYVPCS}" | awk '{split($0,a,","); print a[2]}')

      if [ "${VPCID}" != "" ] && [ "${CIDRBLOCK}" != "" ]; then
            GROUPID="$(aws ec2 create-security-group                    \
                  --group-name ${OWNER}'cdp-trial-security-group'       \
                  --description 'CDP Private Cloud 60 day trial'        \
                  --vpc-id $VPCID                                       \
                  | jq -r '. | "\(.GroupId)"')"

            if [ "${GROUPID}" != "" ]; then
                  aws ec2 authorize-security-group-ingress              \
                        --group-id $GROUPID                             \
                        --protocol tcp --port 22 --cidr 0.0.0.0/0

                  aws ec2 authorize-security-group-ingress              \
                        --group-id $GROUPID                             \
                        --protocol icmp                                 \
                        --port -1 --source-group $GROUPID

                  aws ec2 authorize-security-group-ingress              \
                        --group-id $GROUPID                             \
                        --protocol tcp --port 0-65535 --cidr $CIDRBLOCK

                  aws ec2 authorize-security-group-ingress              \
                        --group-id $GROUPID                             \
                        --protocol tcp --port 0-65535 --cidr $LOCAL_IP/32
            else
                  printf " [ERROR: BAD VALUE for Security Group: (${GROUPID})"
                  exit
            fi
      else
            printf " [ERROR: BAD VALUEs for VpcId and CidrBlock: (${VPCID},${CIDRBLOCK})"
            exit
      fi
fi
printf "\n"



#------------------------------------------
#     CREATE EC2 INSTANCES
#------------------------------------------
printf "Creating instances"
SUBNET="$(aws ec2 describe-subnets                    \
      --filters "Name=vpc-id,Values=${VPCID}"         \
      | jq -r '.Subnets[] | "\(.SubnetId)"'           \
      | head -n 1)"

EC2INSTANCES=($(aws ec2 run-instances --image-id ami-0bc06212a56393ee1 --count $NUM_NODES --instance-type m5.2xlarge --key-name ${OWNER}'cdp-trial-key' --security-group-id $GROUPID --subnet-id $SUBNET --block-device-mappings 'DeviceName=/dev/sda1,Ebs={DeleteOnTermination=true,VolumeSize=100,Encrypted=false}' | jq  -r '.Instances[] | .InstanceId'))
sleep 1
for instance in "${EC2INSTANCES[@]}"; do
      count=0
      complete=false
      while [ "$count" -lt "$RETRY" ] &&  [ "$complete" = "false" ]; do
            MYSTATE=$(aws ec2 describe-instances --instance-ids ${instance} | jq  -r '.Reservations[].Instances[].State.Name')
            if [ "$MYSTATE" != "running" ]; then
                  printf " ."
                  count=$(( count + 1 ))
                  sleep 1
            else
                  complete=true
            fi
      done

      if [ "$count" -ge "$RETRY" ] || [ "$complete" = "false" ]; then
            printf " [FAILED]\n"
      fi
done
printf "\n"



#------------------------------------------
#                 SUMMARY
#------------------------------------------
printf "\n\n"
printf "SUMMARY:\n"
printf "\t%15s %-15s\n" "IP Address:"     "${LOCAL_IP}"
printf "\t%15s %-15s\n" "Key Pair:"       "${KEYPAIR}"
printf "\t%15s %-15s\n" "VPCID:"          "${VPCID}"
printf "\t%15s %-15s\n" "Security Group:" "${GROUPID}"
printf "\t%15s %-15s\n" "Subnet:"         "${SUBNET}"

EC2INSTANCES=($(aws ec2 describe-instances --filters "Name=key-name, Values=${OWNER}cdp-trial-key" "Name=instance-state-name, Values=running" | jq  -r '.Reservations[].Instances[] | "\(.InstanceId),\(.PublicIpAddress),\(.PublicDnsName),\(.PrivateIpAddress),\(.PrivateDnsName)"'))
printf "\n\t%15s\n"       "Instances:"
for ((i = 0; i < ${#EC2INSTANCES[@]}; i++)); do
   element="${EC2INSTANCES[$i]}"
   InstanceId=$(echo "${element}" | awk '{split($0,a,","); print a[1]}')
   PublicIpAddress=$(echo "${element}" | awk '{split($0,a,","); print a[2]}')
   PublicDnsName=$(echo "${element}" | awk '{split($0,a,","); print a[3]}')
   PrivateIpAddress=$(echo "${element}" | awk '{split($0,a,","); print a[4]}')
   PrivateDnsName=$(echo "${element}" | awk '{split($0,a,","); print a[5]}')

   if [ $i = 0 ]; then
      printf "\t\t%15s" "Primary Node: "
   else
      printf "\t\t%15s" "Node: "
   fi

   printf "${InstanceId}\n"
   printf "\t\t%15s %-15s %s\n" "Public IP: " "${PublicIpAddress}" "${PublicDnsName}"
   printf "\t\t%15s %-15s %s\n" "Private IP: " "${PrivateIpAddress}" "${PrivateDnsName}"
   printf "\n"
done

printf "\t%15s" "CDP Hosts: "
for ((i = 0; i < ${#EC2INSTANCES[@]}; i++)); do
   element="${EC2INSTANCES[$i]}"
   PrivateIpAddress=$(echo "${element}" | awk '{split($0,a,","); print a[4]}')
   printf " %-15s" "${PrivateIpAddress}"
done

printf "\n\n\nACTION ITEMS:\n\n\n"
printf "1. APPEND TO HOST FILE:\n"
printf "# --------------------------------\n"
printf "# - CLOUDERA DATA PLATFORM TRIAL -\n"
printf "# --------------------------------\n"
for ((i = 0; i < ${#EC2INSTANCES[@]}; i++)); do
   element="${EC2INSTANCES[$i]}"
   PublicIpAddress=$(echo "${element}" | awk '{split($0,a,","); print a[2]}')
   PrivateDnsName=$(echo "${element}" | awk '{split($0,a,","); print a[5]}')

   printf "${PublicIpAddress}\t${PrivateDnsName}"
   if [ $i = 0 ]; then
      printf "\tcdptrial"
   fi
   printf "\n"
done

printf "\n\n2. RUN COMMANDS ON EACH NODE:\n"
for ((i = ${#EC2INSTANCES[@]}-1; i >= 0; i--)); do
   element="${EC2INSTANCES[$i]}"
   PublicIpAddress=$(echo "${element}" | awk '{split($0,a,","); print a[2]}')

   if [ $i = 0 ]; then
      printf "%15s %-15s\n" "Primary Node:" "${PublicIpAddress}"
      printf "\t\tssh -i '${KEYPAIR}' centos@${PublicIpAddress}\n"
      printf "\t\tsudo sysctl vm.swappiness=10\n"
      printf "\t\tsudo sed -i 's/SELINUX=.*/SELINUX=disabled/' /etc/selinux/config\n"
      printf "\t\tsudo yum install -y wget\n\n"
   else
      printf "%15s %-15s\n" "Node:" "${PublicIpAddress}"
      printf "\t\tssh -i '${KEYPAIR}' centos@${PublicIpAddress}\n"
      printf "\t\tsudo sysctl vm.swappiness=10\n"
      printf "\t\texit\n\n"
   fi
done
