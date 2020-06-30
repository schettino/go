#! /usr/bin/env bash
set -e

BASE_DB_URL=postgres://postgres:yL15LwgWSkGm@horizon-captive-core-experiment-fons.cmsk9uo5u6im.us-east-1.rds.amazonaws.com:5432

dump_horizon_db() {
	echo "dumping history_effects"
	psql "$BASE_DB_URL/${2}" -t -A -F"," --variable="FETCH_COUNT=100" -c "select history_effects.history_operation_id, history_effects.order, type, details, history_accounts.address from history_effects left join history_accounts on history_accounts.id = history_effects.history_account_id order by history_operation_id asc, \"order\" asc" > "${1}_effects"
	echo "dumping history_ledgers"
	psql "$BASE_DB_URL/${2}" -t -A -F"," --variable="FETCH_COUNT=100" -c "select sequence, ledger_hash, previous_ledger_hash, transaction_count, operation_count, closed_at, id, total_coins, fee_pool, base_fee, base_reserve, max_tx_set_size, protocol_version, ledger_header, successful_transaction_count, failed_transaction_count from history_ledgers order by sequence asc" > "${1}_ledgers"
	echo "dumping history_operations"
	psql "$BASE_DB_URL/${2}" -t -A -F"," --variable="FETCH_COUNT=100" -c "select * from history_operations order by id asc" > "${1}_operations"
	echo "dumping history_operation_participants"
	psql "$BASE_DB_URL/${2}" -t -A -F"," --variable="FETCH_COUNT=100" -c "select history_operation_id, address from history_operation_participants left join history_accounts on history_accounts.id = history_operation_participants.history_account_id order by history_operation_id asc, address asc" > "${1}_operation_participants"
	echo "dumping history_trades"
	psql "$BASE_DB_URL/${2}" -t -A -F"," --variable="FETCH_COUNT=100" -c "select history_trades.history_operation_id, history_trades.order, history_trades.ledger_closed_at, history_trades.offer_id, CASE WHEN history_trades.base_is_seller THEN history_trades.price_n ELSE history_trades.price_d END, CASE WHEN history_trades.base_is_seller THEN history_trades.price_d ELSE history_trades.price_n END, CASE WHEN history_trades.base_is_seller THEN history_trades.base_offer_id ELSE history_trades.counter_offer_id END, CASE WHEN history_trades.base_is_seller THEN history_trades.counter_offer_id ELSE history_trades.base_offer_id END, CASE WHEN history_trades.base_is_seller THEN baccount.address ELSE caccount.address END, CASE WHEN history_trades.base_is_seller THEN caccount.address ELSE baccount.address END, CASE WHEN history_trades.base_is_seller THEN basset.asset_type ELSE casset.asset_type END, CASE WHEN history_trades.base_is_seller THEN basset.asset_code ELSE casset.asset_code END, CASE WHEN history_trades.base_is_seller THEN basset.asset_issuer ELSE casset.asset_issuer END, CASE WHEN history_trades.base_is_seller THEN casset.asset_type ELSE basset.asset_type END, CASE WHEN history_trades.base_is_seller THEN casset.asset_code ELSE basset.asset_code END, CASE WHEN history_trades.base_is_seller THEN casset.asset_issuer ELSE basset.asset_issuer END from history_trades left join history_accounts baccount on baccount.id = history_trades.base_account_id left join history_accounts caccount on caccount.id = history_trades.counter_account_id left join history_assets basset on basset.id = history_trades.base_asset_id left join history_assets casset on casset.id = history_trades.counter_asset_id order by history_operation_id asc, \"order\" asc" > "${1}_trades"
	echo "dumping history_transactions"
	psql "$BASE_DB_URL/${2}" -t -A -F"," --variable="FETCH_COUNT=100" -c "select transaction_hash, ledger_sequence, application_order, account, account_sequence, max_fee, operation_count, id, tx_envelope, tx_result, signatures, memo_type, memo, time_bounds, successful, fee_charged from history_transactions order by id asc" > "${1}_transactions"
	echo "dumping history_transaction_participants"
	psql "$BASE_DB_URL/${2}" -t -A -F"," --variable="FETCH_COUNT=100" -c "select history_transaction_id, address from history_transaction_participants left join history_accounts on history_accounts.id = history_transaction_participants.history_account_id order by history_transaction_id, address" > "${1}_transaction_participants"
}


function compare() {
  local expected="$1"
  local actual="$2"

  # Files can be very large, leading to `diff` running out of memory.
  # As a workaround, since files are expected to be identical,
  # we compare the hashes first.
  local hash=$(shasum -a 256 "$expected" | cut -f 1 -d ' ')
  local check_command="$hash  $actual"

  if ! ( echo "$check_command" | shasum -a 256 -c ); then
    diff --speed-large-files "$expected" "$actual"
  fi
}

# Ingest without parallelization
dump_horizon_db "exp_history" horizon

echo "Done dump_horizon_db exp_history"



# Ingest with parallelization

dump_horizon_db "legacy_history" horizon_parallel
echo "Done dump_horizon_db legacy_history"

compare legacy_history_effects exp_history_effects
compare legacy_history_ledgers exp_history_ledgers
compare legacy_history_operations exp_history_operations
compare legacy_history_operation_participants exp_history_operation_participants
compare legacy_history_trades exp_history_trades
compare legacy_history_transactions exp_history_transactions
compare legacy_history_transaction_participants exp_history_transaction_participants
