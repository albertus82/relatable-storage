package io.github.albertus82.storage;

import javax.sql.DataSource;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@ComponentScan
@EnableTransactionManagement
public class TestConfig {

	@Bean
	DataSource dataSource() {
		return new DriverManagerDataSource(
		//@formatter:off
				"jdbc:h2:mem:" + getClass().getSimpleName() + ";DB_CLOSE_DELAY=-1"
//				"jdbc:oracle:thin:@localhost:1521/XEPDB1", "test", "test"
		//@formatter:on
		);
	}

	@Bean
	PlatformTransactionManager transactionManager() {
		return new DataSourceTransactionManager(dataSource());
	}

	@Bean
	JdbcTemplate jdbcTemplate() {
		return new JdbcTemplate(dataSource());
	}

}
