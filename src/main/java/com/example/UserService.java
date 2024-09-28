package com.example;

import java.util.ArrayList;
import java.util.List;

public class UserService {

    public static List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        users.add(new User(1, "Jaga"));
        users.add(new User(2, "Deva"));
        users.add(new User(3, "V"));

        return users;
    }



}
