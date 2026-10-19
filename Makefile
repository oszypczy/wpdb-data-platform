OLIST_DIR := data/olist
OLIST_URL := https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce
OLIST_STAMP := $(OLIST_DIR)/.downloaded

.PHONY: help data clean-data

help:
	@echo "make data        - download the Olist dataset into $(OLIST_DIR)/ (skipped if present)"
	@echo "make clean-data  - remove downloaded CSV files"

data: $(OLIST_STAMP)

$(OLIST_STAMP):
	@echo "Downloading Olist dataset..."
	@tmp=$$(mktemp -d) && \
	curl -fsSL -o $$tmp/olist.zip $(OLIST_URL) && \
	unzip -oq $$tmp/olist.zip -d $(OLIST_DIR) -x olist_geolocation_dataset.csv && \
	rm -rf $$tmp
	@touch $@
	@echo "Done: $$(ls $(OLIST_DIR)/*.csv | wc -l | tr -d ' ') CSV files in $(OLIST_DIR)/"

clean-data:
	rm -f $(OLIST_DIR)/*.csv $(OLIST_STAMP)
