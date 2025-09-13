## link to data on datadisk
lsblk -o NAME,HCTL,SIZE,MOUNTPOINT
sudo mkdir -p /mnt/data
sudo mount /dev/sda1 /mnt/data
df -h | grep /mnt/data





python make_webdataset_shards.py \
  --input_jsonl /home/azureuser/BiomedCLIP_data_pipeline/_results/data/pubmed_parsed_data.jsonl \
  --out_dir     /mnt/datadisk/biomedclip_wds/train \
  --prefix      biomedclip-train \
  --shard_size  10000 \
  --allowlist_file /home/azureuser/BiomedCLIP_data_pipeline/_results/data/pubmed_open_access_file_list_train.txt


mkdir -p /mnt/data/pmc/{train,dev,test}/{compressed,extracted,logs}


python process.py \
  --mode devtest \
  --dev_list  /home/azureuser/disk/_results/data/pubmed_open_access_file_list_dev.txt \
  --test_list /home/azureuser/disk/_results/data/pubmed_open_access_file_list_test.txt \
  --out_root  /home/azureuser/disk/_results/data/pmc15_cc12m_like


python process.py \
  --mode devtest \
  --test_list /home/azureuser/disk/_results/data/pubmed_open_access_file_list_test.txt \
  --out_root  /home/azureuser/disk/_results/data/pmc15_cc12m_like


python process.py \
  --mode devtest \
  --dev_list  /home/azureuser/disk/_results/data/pubmed_open_access_file_list_dev.txt \
  --out_root  /home/azureuser/disk/_results/data/pmc15_cc12m_like

python process.py \
  --mode shards \
  --shard_dir /home/azureuser/disk/_results/data/shards_train \
  --out_root  /home/azureuser/disk/_results/data/pmc15_cc12m_like


python process_parallel.py \
  --mode shards \
  --shard_dir /home/azureuser/disk/_results/data/shards_train \
  --out_root  /home/azureuser/disk/_results/data/pmc15_cc12m_like \
  --num_workers 8


### upload to azure cloud
SRC=/home/azureuser/disk/_results/data/pmc15_cc12m_like
RG=rg-pmc15
LOC=centralus     # or your region
STG=pmc15$RANDOM     # storage account name must be globally unique
CONTAINER=pmc15




# 1) Resource group + storage + container
az group create -n $RG -l $LOC
az storage account create -g $RG -n $STG -l $LOC --sku Standard_LRS --kind StorageV2
az storage container create --account-name $STG -n $CONTAINER

# 2) Create a 7-day SAS on the container
EXP=$(date -u -d "+7 days" '+%Y-%m-%dT%H:%MZ')
SAS=$(az storage container generate-sas \
  --account-name "$STG" \
  -n "$CONTAINER" \
  --permissions acdlrw \
  --expiry "$EXP" \
  --auth-mode login \
  --as-user \
  -o tsv)
DEST="https://$STG.blob.core.windows.net/$CONTAINER?$SAS"
echo "$DEST"

# 3) Install AzCopy (user-local, no sudo)
cd ~
wget -O azcopy.tgz https://aka.ms/downloadazcopy-v10-linux
tar -xvf azcopy.tgz
mkdir -p ~/bin
mv azcopy_linux_amd64_*/azcopy ~/bin/
echo 'export PATH=$HOME/bin:$PATH' >> ~/.bashrc
export PATH=$HOME/bin:$PATH
azcopy --version

# 4) Upload (one time)
# JSONLs
azcopy copy "$SRC/dev.jsonl"  "$DEST/dev/dev.jsonl"
azcopy copy "$SRC/test.jsonl" "$DEST/test/test.jsonl"

# Images
azcopy copy "$SRC/images/dev"  "$DEST/dev/images"  --recursive=true
azcopy copy "$SRC/images/test" "$DEST/test/images" --recursive=true

# (Optional) Train shards
for f in "$SRC"/train_shard-*.jsonl; do
  azcopy copy "$f" "$DEST/train/jsonl/"
done
azcopy copy "$SRC/images/train" "$DEST/train/images" --recursive=true

# 5) Verify a few blobs
az storage blob list --account-name $STG -c $CONTAINER --prefix "dev/images" -o table | head


##
## the correct way is to use the gui to create storage account and then container and then generate sas token
## then use the url to upload data using azcopy command

## blob sas token 
## sp=racwdl&st=2025-09-13T09:53:19Z&se=2025-09-30T18:08:19Z&spr=https&sv=2024-11-04&sr=c&sig=m3qcnBqETyr0srA2QXIu4TVLs2FJHBNtb%2FX1HrAICn4%3D
## url 
## https://biomedclip1234.blob.core.windows.net/data?sp=racwdl&st=2025-09-13T09:53:19Z&se=2025-09-30T18:08:19Z&spr=https&sv=2024-11-04&sr=c&sig=m3qcnBqETyr0srA2QXIu4TVLs2FJHBNtb%2FX1HrAICn4%3D

## correct command now.
azcopy copy "/home/azureuser/disk/_results/data/pmc15_cc12m_like" "https://biomedclip1234.blob.core.windows.net/data?sp=racwdl&st=2025-09-13T09:53:19Z&se=2025-09-30T18:08:19Z&spr=https&sv=2024-11-04&sr=c&sig=m3qcnBqETyr0srA2QXIu4TVLs2FJHBNtb%2FX1HrAICn4%3D" --recursive --overwrite=ifSourceNewer



azcopy jobs show $JOB --with-status=Failed
