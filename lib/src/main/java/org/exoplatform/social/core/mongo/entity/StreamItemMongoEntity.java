/*
 * Copyright (C) 2003-2013 eXo Platform SAS.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.exoplatform.social.core.mongo.entity;

import org.exoplatform.social.core.storage.query.PropertyLiteralExpression;

/**
 * Created by The eXo Platform SAS
 * Author : eXoPlatform
 *          exo@exoplatform.com
 * Nov 21, 2013  
 */
public class StreamItemMongoEntity {

  public static final PropertyLiteralExpression<String> id = new PropertyLiteralExpression<String>(String.class, "_id");
  public static final PropertyLiteralExpression<String> activityId = new PropertyLiteralExpression<String>(String.class, "activityId");
  public static final PropertyLiteralExpression<String> owner = new PropertyLiteralExpression<String>(String.class, "owner");
  public static final PropertyLiteralExpression<String> poster = new PropertyLiteralExpression<String>(String.class, "poster");
  //LIKER, COMMENTER or MENTIONER
  public static final PropertyLiteralExpression<String> viewerId = new PropertyLiteralExpression<String>(String.class, "viewerId");
  //CONNECTION, LIKER, MENTIONER, COMMENTER
  public static final PropertyLiteralExpression<String[]> viewerTypes = new PropertyLiteralExpression<String[]>(String[].class, "viewerTypes");
  public static final PropertyLiteralExpression<Boolean> hiable = new PropertyLiteralExpression<Boolean>(Boolean.class, "hiable");
  public static final PropertyLiteralExpression<Boolean> lockable = new PropertyLiteralExpression<Boolean>(Boolean.class, "lockable");
  public static final PropertyLiteralExpression<Long> time = new PropertyLiteralExpression<Long>(Long.class, "time");
  public static final PropertyLiteralExpression<String> actionNo = new PropertyLiteralExpression<String>(String.class, "actionNo");
  
  public enum ViewerType { LIKER, COMMENTER, POSTER, MENTIONER }
  
}
