/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.orioninc.migration;

import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Data;

/**
 *
 * @author memin
 */
@Data
public class SubscriptionInfo {

    private String realm;
    private TurnCredentials turnCredentials;
    private Set<String> services;
    private String notificationToken;
    private String notificatonClientCorelator;
    private Map<String, List<String>> spidrHeader;
    private String channelId;
    
}
