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

