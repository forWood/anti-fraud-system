package com.bank.risk.am;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@SpringBootApplication
@EnableDiscoveryClient
public class AlertManagementApplication {
    public static void main(String[] args) {
        SpringApplication.run(AlertManagementApplication.class, args);
    }
}
