package com.barabanov.specific.features.service;

import com.barabanov.specific.features.kafka.dto.Response;


public interface ResponseMsgService {

    void handle(Response response);
}
