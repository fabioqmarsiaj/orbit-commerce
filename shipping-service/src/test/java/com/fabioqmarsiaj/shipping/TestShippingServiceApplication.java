package com.fabioqmarsiaj.shipping;

import org.springframework.boot.SpringApplication;

public class TestShippingServiceApplication {

	public static void main(String[] args) {
		SpringApplication.from(ShippingServiceApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
