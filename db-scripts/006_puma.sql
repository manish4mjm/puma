CREATE TABLE "time_series".equity_returns (
	equity_id serial NOT NULL,
	trading_date date not null,
	data_source varchar NOT NULL,
	daily_return numeric(36,15) NOT NULL,
	daily_log_return numeric(36,15) NOT NULL,
	cumulative_return numeric(36,15) NOT null,
	CONSTRAINT equity_id_fkey FOREIGN KEY (equity_id) REFERENCES ref.equity(equity_id)
)
INHERITS ("ref".last_updated_base);
