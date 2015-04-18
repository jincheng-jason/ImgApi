package com.java.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by lijc on 15/4/18.
 */
@RestController
public class HomeController {

    @RequestMapping(value = "/",method = RequestMethod.GET)
    public String hello(){
        return "hello world!";
    }

}
