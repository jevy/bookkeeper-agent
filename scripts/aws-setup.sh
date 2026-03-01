#!/usr/bin/env bash
#
# Idempotent setup of AWS resources for the Bookkeeper email digest feature.
# Resources: SES domain identity, Route53 records, S3 bucket, SES receipt rule, IAM user + policy.
#
# Usage:
#   scripts/aws-setup.sh              # provision all resources
#   scripts/aws-setup.sh --dry-run    # show what would be created (no changes)
#
# Prerequisites:
#   - aws CLI configured with credentials that can create SES, S3, Route53, IAM resources
#   - A Route53 hosted zone for the parent domain (jevy.org)

set -euo pipefail

# ─── Configuration ──────────────────────────────────────────────────────────────

DOMAIN="bookkeeper.jevy.org"
PARENT_DOMAIN="jevy.org"
S3_BUCKET="bookkeeper-emails-jevy"
AWS_REGION="us-east-1"
IAM_USER="bookkeeper-ses-s3"
IAM_POLICY_NAME="bookkeeper-ses-s3-policy"
SES_RULE_SET="bookkeeper-rules"
SES_RULE_NAME="bookkeeper-s3-ingest"
RECIPIENT="digest@${DOMAIN}"

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=true
    echo "=== DRY RUN — no changes will be made ==="
    echo
fi

AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# ─── Helpers ────────────────────────────────────────────────────────────────────

info()    { echo "  [ok]  $*"; }
create()  { echo "  [new] $*"; }
skip()    { echo "  [skip] $*"; }

get_hosted_zone_id() {
    aws route53 list-hosted-zones-by-name \
        --dns-name "${PARENT_DOMAIN}." \
        --query "HostedZones[?Name=='${PARENT_DOMAIN}.'].Id" \
        --output text | sed 's|/hostedzone/||'
}

# ─── 1. SES Domain Identity ────────────────────────────────────────────────────

echo "=== SES Domain Identity ==="

EXISTING_IDENTITIES=$(aws ses list-identities --identity-type Domain --query "Identities" --output text --region "$AWS_REGION")
if echo "$EXISTING_IDENTITIES" | grep -qw "$DOMAIN"; then
    info "SES domain identity '$DOMAIN' already exists"
    VERIFICATION_TOKEN=$(aws ses get-identity-verification-attributes \
        --identities "$DOMAIN" \
        --query "VerificationAttributes.\"${DOMAIN}\".VerificationToken" \
        --output text --region "$AWS_REGION")
else
    if $DRY_RUN; then
        create "Would create SES domain identity '$DOMAIN'"
        VERIFICATION_TOKEN="<will-be-generated>"
    else
        VERIFICATION_TOKEN=$(aws ses verify-domain-identity \
            --domain "$DOMAIN" \
            --query "VerificationToken" \
            --output text --region "$AWS_REGION")
        create "Created SES domain identity '$DOMAIN'"
    fi
fi

echo

# ─── 2. Route53 Records ────────────────────────────────────────────────────────

echo "=== Route53 DNS Records ==="

HOSTED_ZONE_ID=$(get_hosted_zone_id)
if [[ -z "$HOSTED_ZONE_ID" ]]; then
    echo "ERROR: Could not find hosted zone for $PARENT_DOMAIN"
    exit 1
fi
info "Using hosted zone $HOSTED_ZONE_ID for $PARENT_DOMAIN"

# TXT record for SES verification
EXISTING_TXT=$(aws route53 list-resource-record-sets \
    --hosted-zone-id "$HOSTED_ZONE_ID" \
    --query "ResourceRecordSets[?Name=='_amazonses.${DOMAIN}.' && Type=='TXT']" \
    --output text)

if [[ -n "$EXISTING_TXT" ]]; then
    info "TXT record _amazonses.${DOMAIN} already exists"
else
    if $DRY_RUN; then
        create "Would create TXT record _amazonses.${DOMAIN}"
    else
        aws route53 change-resource-record-sets \
            --hosted-zone-id "$HOSTED_ZONE_ID" \
            --change-batch '{
                "Changes": [{
                    "Action": "UPSERT",
                    "ResourceRecordSet": {
                        "Name": "_amazonses.'"$DOMAIN"'",
                        "Type": "TXT",
                        "TTL": 300,
                        "ResourceRecords": [{"Value": "\"'"$VERIFICATION_TOKEN"'\""}]
                    }
                }]
            }' > /dev/null
        create "Created TXT record _amazonses.${DOMAIN}"
    fi
fi

# MX record for inbound email
EXISTING_MX=$(aws route53 list-resource-record-sets \
    --hosted-zone-id "$HOSTED_ZONE_ID" \
    --query "ResourceRecordSets[?Name=='${DOMAIN}.' && Type=='MX']" \
    --output text)

if [[ -n "$EXISTING_MX" ]]; then
    info "MX record ${DOMAIN} already exists"
else
    if $DRY_RUN; then
        create "Would create MX record ${DOMAIN}"
    else
        aws route53 change-resource-record-sets \
            --hosted-zone-id "$HOSTED_ZONE_ID" \
            --change-batch '{
                "Changes": [{
                    "Action": "UPSERT",
                    "ResourceRecordSet": {
                        "Name": "'"$DOMAIN"'",
                        "Type": "MX",
                        "TTL": 300,
                        "ResourceRecords": [{"Value": "10 inbound-smtp.'"$AWS_REGION"'.amazonaws.com"}]
                    }
                }]
            }' > /dev/null
        create "Created MX record ${DOMAIN} → inbound-smtp.${AWS_REGION}.amazonaws.com"
    fi
fi

echo

# ─── 3. S3 Bucket ──────────────────────────────────────────────────────────────

echo "=== S3 Bucket ==="

if aws s3api head-bucket --bucket "$S3_BUCKET" 2>/dev/null; then
    info "S3 bucket '$S3_BUCKET' already exists"
else
    if $DRY_RUN; then
        create "Would create S3 bucket '$S3_BUCKET'"
    else
        aws s3api create-bucket --bucket "$S3_BUCKET" --region "$AWS_REGION"
        create "Created S3 bucket '$S3_BUCKET'"
    fi
fi

# Bucket policy allowing SES to write to inbox/
BUCKET_POLICY='{
    "Version": "2012-10-17",
    "Statement": [{
        "Sid": "AllowSESPuts",
        "Effect": "Allow",
        "Principal": {"Service": "ses.amazonaws.com"},
        "Action": "s3:PutObject",
        "Resource": "arn:aws:s3:::'"$S3_BUCKET"'/inbox/*",
        "Condition": {
            "StringEquals": {"AWS:SourceAccount": "'"$AWS_ACCOUNT_ID"'"}
        }
    }]
}'

EXISTING_POLICY=$(aws s3api get-bucket-policy --bucket "$S3_BUCKET" --query Policy --output text 2>/dev/null || echo "")
if [[ -n "$EXISTING_POLICY" ]] && echo "$EXISTING_POLICY" | grep -q "AllowSESPuts"; then
    info "S3 bucket policy (AllowSESPuts) already set"
else
    if $DRY_RUN; then
        create "Would set S3 bucket policy for SES write access"
    else
        aws s3api put-bucket-policy --bucket "$S3_BUCKET" --policy "$BUCKET_POLICY"
        create "Set S3 bucket policy for SES write access"
    fi
fi

echo

# ─── 4. SES Receipt Rule ───────────────────────────────────────────────────────

echo "=== SES Receipt Rule ==="

# Create rule set if it doesn't exist
EXISTING_RULE_SETS=$(aws ses list-receipt-rule-sets \
    --query "RuleSets[].Name" --output text --region "$AWS_REGION")

if echo "$EXISTING_RULE_SETS" | grep -qw "$SES_RULE_SET"; then
    info "SES receipt rule set '$SES_RULE_SET' already exists"
else
    if $DRY_RUN; then
        create "Would create SES receipt rule set '$SES_RULE_SET'"
    else
        aws ses create-receipt-rule-set \
            --rule-set-name "$SES_RULE_SET" --region "$AWS_REGION"
        create "Created SES receipt rule set '$SES_RULE_SET'"
    fi
fi

# Activate the rule set
ACTIVE_RULE_SET=$(aws ses describe-active-receipt-rule-set \
    --query "Metadata.Name" --output text --region "$AWS_REGION" 2>/dev/null || echo "None")

if [[ "$ACTIVE_RULE_SET" == "$SES_RULE_SET" ]]; then
    info "SES rule set '$SES_RULE_SET' is already active"
else
    if $DRY_RUN; then
        create "Would activate SES rule set '$SES_RULE_SET' (currently: $ACTIVE_RULE_SET)"
    else
        aws ses set-active-receipt-rule-set \
            --rule-set-name "$SES_RULE_SET" --region "$AWS_REGION"
        create "Activated SES rule set '$SES_RULE_SET'"
    fi
fi

# Create or update receipt rule
EXISTING_RULES=$(aws ses describe-receipt-rule-set \
    --rule-set-name "$SES_RULE_SET" \
    --query "Rules[].Name" --output text --region "$AWS_REGION" 2>/dev/null || echo "")

if echo "$EXISTING_RULES" | grep -qw "$SES_RULE_NAME"; then
    info "SES receipt rule '$SES_RULE_NAME' already exists"
else
    if $DRY_RUN; then
        create "Would create SES receipt rule '$SES_RULE_NAME' → s3://${S3_BUCKET}/inbox/"
    else
        aws ses create-receipt-rule \
            --rule-set-name "$SES_RULE_SET" \
            --region "$AWS_REGION" \
            --rule '{
                "Name": "'"$SES_RULE_NAME"'",
                "Enabled": true,
                "Recipients": ["'"$RECIPIENT"'"],
                "Actions": [{
                    "S3Action": {
                        "BucketName": "'"$S3_BUCKET"'",
                        "ObjectKeyPrefix": "inbox/"
                    }
                }],
                "ScanEnabled": true
            }'
        create "Created SES receipt rule '$SES_RULE_NAME' → s3://${S3_BUCKET}/inbox/"
    fi
fi

echo

# ─── 5. IAM User + Policy ──────────────────────────────────────────────────────

echo "=== IAM User & Policy ==="

EXISTING_USERS=$(aws iam list-users --query "Users[].UserName" --output text)
if echo "$EXISTING_USERS" | grep -qw "$IAM_USER"; then
    info "IAM user '$IAM_USER' already exists"
else
    if $DRY_RUN; then
        create "Would create IAM user '$IAM_USER'"
    else
        aws iam create-user --user-name "$IAM_USER" > /dev/null
        create "Created IAM user '$IAM_USER'"
    fi
fi

IAM_POLICY_DOC='{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "SESSend",
            "Effect": "Allow",
            "Action": ["ses:SendEmail", "ses:SendRawEmail"],
            "Resource": "*",
            "Condition": {
                "StringLike": {"ses:FromAddress": "*@'"$DOMAIN"'"}
            }
        },
        {
            "Sid": "S3ReadWrite",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
                "s3:CopyObject"
            ],
            "Resource": [
                "arn:aws:s3:::'"$S3_BUCKET"'",
                "arn:aws:s3:::'"$S3_BUCKET"'/*"
            ]
        }
    ]
}'

POLICY_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${IAM_POLICY_NAME}"
EXISTING_POLICY=$(aws iam get-policy --policy-arn "$POLICY_ARN" 2>/dev/null && echo "exists" || echo "")

if [[ -n "$EXISTING_POLICY" ]]; then
    info "IAM policy '$IAM_POLICY_NAME' already exists"
    # Update the policy document in case it changed
    if ! $DRY_RUN; then
        # Create new version and set as default
        VERSIONS=$(aws iam list-policy-versions --policy-arn "$POLICY_ARN" \
            --query "Versions[?!IsDefaultVersion].VersionId" --output text)
        # Delete non-default versions if at limit (max 5)
        VERSION_COUNT=$(aws iam list-policy-versions --policy-arn "$POLICY_ARN" \
            --query "length(Versions)" --output text)
        if [[ "$VERSION_COUNT" -ge 5 ]]; then
            OLDEST=$(aws iam list-policy-versions --policy-arn "$POLICY_ARN" \
                --query "Versions[?!IsDefaultVersion] | [-1].VersionId" --output text)
            aws iam delete-policy-version --policy-arn "$POLICY_ARN" --version-id "$OLDEST"
        fi
        aws iam create-policy-version \
            --policy-arn "$POLICY_ARN" \
            --policy-document "$IAM_POLICY_DOC" \
            --set-as-default > /dev/null
        info "Updated IAM policy to latest document"
    fi
else
    if $DRY_RUN; then
        create "Would create IAM policy '$IAM_POLICY_NAME'"
    else
        aws iam create-policy \
            --policy-name "$IAM_POLICY_NAME" \
            --policy-document "$IAM_POLICY_DOC" > /dev/null
        create "Created IAM policy '$IAM_POLICY_NAME'"
    fi
fi

# Attach policy to user
ATTACHED=$(aws iam list-attached-user-policies --user-name "$IAM_USER" \
    --query "AttachedPolicies[?PolicyName=='${IAM_POLICY_NAME}']" --output text 2>/dev/null || echo "")

if [[ -n "$ATTACHED" ]]; then
    info "Policy already attached to user '$IAM_USER'"
else
    if $DRY_RUN; then
        create "Would attach policy '$IAM_POLICY_NAME' to user '$IAM_USER'"
    else
        aws iam attach-user-policy \
            --user-name "$IAM_USER" \
            --policy-arn "$POLICY_ARN"
        create "Attached policy to user '$IAM_USER'"
    fi
fi

# Check for existing access keys
EXISTING_KEYS=$(aws iam list-access-keys --user-name "$IAM_USER" \
    --query "AccessKeyMetadata[].AccessKeyId" --output text 2>/dev/null || echo "")

if [[ -n "$EXISTING_KEYS" ]]; then
    info "IAM user '$IAM_USER' already has access key(s): $EXISTING_KEYS"
    echo
    echo "  To rotate credentials, delete the existing key and re-run this script:"
    echo "    aws iam delete-access-key --user-name $IAM_USER --access-key-id <KEY_ID>"
else
    if $DRY_RUN; then
        create "Would create access key for user '$IAM_USER'"
    else
        echo
        echo "  Creating access key for '$IAM_USER'..."
        CREDENTIALS=$(aws iam create-access-key --user-name "$IAM_USER" --query "AccessKey")
        ACCESS_KEY_ID=$(echo "$CREDENTIALS" | jq -r '.AccessKeyId')
        SECRET_ACCESS_KEY=$(echo "$CREDENTIALS" | jq -r '.SecretAccessKey')
        create "Created access key for user '$IAM_USER'"
        echo
        echo "  ╔══════════════════════════════════════════════════════════════╗"
        echo "  ║  Save these credentials — the secret won't be shown again  ║"
        echo "  ╠══════════════════════════════════════════════════════════════╣"
        echo "  ║  AWS_ACCESS_KEY_ID:     $ACCESS_KEY_ID"
        echo "  ║  AWS_SECRET_ACCESS_KEY: $SECRET_ACCESS_KEY"
        echo "  ╚══════════════════════════════════════════════════════════════╝"
    fi
fi

echo
echo "=== Done ==="
