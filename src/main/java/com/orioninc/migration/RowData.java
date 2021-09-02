/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.orioninc.migration;

import java.util.Date;
import lombok.Data;

/**
 *
 * @author memin
 */
@Data
public class RowData {
    private String service; 
    private String uniqueKey;
    private String version;
    private String username;
    
}
