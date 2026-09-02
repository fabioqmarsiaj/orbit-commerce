package com.fabioqmarsiaj.query;

import org.springframework.boot.SpringApplication;

public class TestQueryServiceApplication {

	public static void main(String[] args) {
		SpringApplication.from(QueryServiceApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
