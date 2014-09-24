/*
 * Copyright (C) 2003-2013 eXo Platform SAS.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.exoplatform.social.core.storage.impl;

import java.util.ArrayList;
import java.util.List;

import org.exoplatform.social.core.activity.model.ExoSocialActivity;
import org.exoplatform.social.core.activity.model.ExoSocialActivityImpl;
import org.exoplatform.social.core.identity.model.Identity;
import org.exoplatform.social.core.identity.provider.OrganizationIdentityProvider;
import org.exoplatform.social.core.identity.provider.SpaceIdentityProvider;
import org.exoplatform.social.core.manager.IdentityManager;
import org.exoplatform.social.core.manager.RelationshipManager;
import org.exoplatform.social.core.mongo.storage.ActivityMongoStorageImpl;
import org.exoplatform.social.core.relationship.model.Relationship;
import org.exoplatform.social.core.space.impl.DefaultSpaceApplicationHandler;
import org.exoplatform.social.core.space.model.Space;
import org.exoplatform.social.core.space.spi.SpaceService;
import org.exoplatform.social.core.storage.api.IdentityStorage;
import org.exoplatform.social.core.test.AbstractCoreTest;

public class ActivityMongoStorageImplTestCase extends AbstractCoreTest {
  
  private IdentityStorage identityStorage;
  private ActivityMongoStorageImpl mongoStorage;
  private RelationshipManager relationshipManager;
  private SpaceService spaceService;
  private IdentityManager identityManager;
  private List<ExoSocialActivity> tearDownActivityList;
  private List<Space> tearDownSpaceList;

  private Identity rootIdentity;
  private Identity johnIdentity;
  private Identity maryIdentity;
  private Identity demoIdentity;
 
  
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    identityStorage = (IdentityStorage) getContainer().getComponentInstanceOfType(IdentityStorage.class);
    mongoStorage = (ActivityMongoStorageImpl) getContainer().getComponentInstanceOfType(ActivityMongoStorageImpl.class);
    relationshipManager = (RelationshipManager) getContainer().getComponentInstanceOfType(RelationshipManager.class);
    spaceService = (SpaceService) getContainer().getComponentInstanceOfType(SpaceService.class);
    identityManager = (IdentityManager) getContainer().getComponentInstanceOfType(IdentityManager.class);
    //
    rootIdentity = new Identity(OrganizationIdentityProvider.NAME, "root");
    johnIdentity = new Identity(OrganizationIdentityProvider.NAME, "john");
    maryIdentity = new Identity(OrganizationIdentityProvider.NAME, "mary");
    demoIdentity = new Identity(OrganizationIdentityProvider.NAME, "demo");
    
    identityStorage.saveIdentity(rootIdentity);
    identityStorage.saveIdentity(johnIdentity);
    identityStorage.saveIdentity(maryIdentity);
    identityStorage.saveIdentity(demoIdentity);

    assertNotNull("rootIdentity.getId() must not be null", rootIdentity.getId());
    assertNotNull("johnIdentity.getId() must not be null", johnIdentity.getId());
    assertNotNull("maryIdentity.getId() must not be null", maryIdentity.getId());
    assertNotNull("demoIdentity.getId() must not be null", demoIdentity.getId());

    tearDownActivityList = new ArrayList<ExoSocialActivity>();
    tearDownSpaceList = new ArrayList<Space>();
  }

  @Override
  protected void tearDown() throws Exception {
    for (ExoSocialActivity activity : tearDownActivityList) {
      mongoStorage.deleteActivity(activity.getId());
    }
    identityStorage.deleteIdentity(rootIdentity);
    identityStorage.deleteIdentity(johnIdentity);
    identityStorage.deleteIdentity(maryIdentity);
    identityStorage.deleteIdentity(demoIdentity);
    //
    for (Space space : tearDownSpaceList) {
      Identity spaceIdentity = identityStorage.findIdentity(SpaceIdentityProvider.NAME, space.getPrettyName());
      if (spaceIdentity != null) {
        identityStorage.deleteIdentity(spaceIdentity);
      }
      spaceService.deleteSpace(space);
    }
    super.tearDown();
  }
  
  public void testSaveActivity() {
    
    ExoSocialActivity activity = createActivity(0);
    //
    mongoStorage.saveActivity(demoIdentity, activity);
    
    assertNotNull(activity.getId());
    
    ExoSocialActivity rs = mongoStorage.getActivity(activity.getId());
    
    //
    assertEquals("demo", rs.getLikeIdentityIds()[0]);
    
    //
    tearDownActivityList.add(activity);
    
  }
  
  public void testUpdateActivity() {
    ExoSocialActivity activity = createActivity(1);
    //
    mongoStorage.saveActivity(demoIdentity, activity);
    
    activity.setTitle("Title after updated");
    
    //update
    mongoStorage.updateActivity(activity);
    
    ExoSocialActivity res = mongoStorage.getActivity(activity.getId());
    
    assertEquals("Title after updated", res.getTitle());
    //
    tearDownActivityList.add(activity);
  }
  
  public void testSaveComment() {
    ExoSocialActivity activity = createActivity(1);
    //
    mongoStorage.saveActivity(demoIdentity, activity);
    ExoSocialActivity comment = createActivity(2);
    comment.setUserId(demoIdentity.getId());
    mongoStorage.saveComment(activity, comment);
    
    ExoSocialActivity got = mongoStorage.getComment(comment.getId());
    assertNotNull(got);
    
  }
  
  public void testMentionersAndCommenters() throws Exception {
    ExoSocialActivity activity = new ExoSocialActivityImpl();
    activity.setTitle("hello @demo @john");
    mongoStorage.saveActivity(rootIdentity, activity);
    tearDownActivityList.add(activity);
    
    ExoSocialActivity got = mongoStorage.getActivity(activity.getId());
    assertNotNull(got);
    assertEquals(2, got.getMentionedIds().length);
    
    ExoSocialActivity comment1 = new ExoSocialActivityImpl();
    comment1.setTitle("comment 1");
    comment1.setUserId(demoIdentity.getId());
    mongoStorage.saveComment(activity, comment1);
    ExoSocialActivity comment2 = new ExoSocialActivityImpl();
    comment2.setTitle("comment 2");
    comment2.setUserId(johnIdentity.getId());
    mongoStorage.saveComment(activity, comment2);
    
    got = mongoStorage.getActivity(activity.getId());
    assertEquals(2, got.getReplyToId().length);
    assertEquals(2, got.getCommentedIds().length);
    
    ExoSocialActivity comment3 = new ExoSocialActivityImpl();
    comment3.setTitle("hello @mary");
    comment3.setUserId(johnIdentity.getId());
    mongoStorage.saveComment(activity, comment3);
    
    got = mongoStorage.getActivity(activity.getId());
    assertEquals(3, got.getReplyToId().length);
    assertEquals(2, got.getCommentedIds().length);
    assertEquals(3, got.getMentionedIds().length);
    
    mongoStorage.deleteComment(activity.getId(), comment3.getId());
    
    got = mongoStorage.getActivity(activity.getId());
    assertEquals(2, got.getReplyToId().length);
    assertEquals(2, got.getCommentedIds().length);
    assertEquals(2, got.getMentionedIds().length);
  }
  
  public void testGetNewerOnActivityFeed() {
    createActivities(3, demoIdentity);
    ExoSocialActivity demoBaseActivity = mongoStorage.getActivityFeed(demoIdentity, 0, 10).get(0);
    assertEquals(0, mongoStorage.getNewerOnActivityFeed(demoIdentity, demoBaseActivity, 10).size());
    assertEquals(0, mongoStorage.getNumberOfNewerOnActivityFeed(demoIdentity, demoBaseActivity));
    //
    createActivities(1, demoIdentity);
    assertEquals(1, mongoStorage.getNewerOnActivityFeed(demoIdentity, demoBaseActivity, 10).size());
    assertEquals(1, mongoStorage.getNumberOfNewerOnActivityFeed(demoIdentity, demoBaseActivity));
    //
    createActivities(2, maryIdentity);
    Relationship demoMaryConnection = relationshipManager.inviteToConnect(demoIdentity, maryIdentity);
    relationshipManager.confirm(maryIdentity, demoIdentity);
    createActivities(2, maryIdentity);
    assertEquals(5, mongoStorage.getNewerOnActivityFeed(demoIdentity, demoBaseActivity, 10).size());
    assertEquals(5, mongoStorage.getNumberOfNewerOnActivityFeed(demoIdentity, demoBaseActivity));
    
    //clear data
    relationshipManager.delete(demoMaryConnection);
  }
  
  public void testGetOlderOnActivityFeed() throws Exception {
    createActivities(3, demoIdentity);
    createActivities(2, maryIdentity);
    Relationship maryDemoConnection = relationshipManager.inviteToConnect(maryIdentity, demoIdentity);
    relationshipManager.confirm(demoIdentity, maryIdentity);
    
    List<ExoSocialActivity> demoActivityFeed = mongoStorage.getActivityFeed(demoIdentity, 0, 10);
    ExoSocialActivity baseActivity = demoActivityFeed.get(4);
    assertEquals(0, mongoStorage.getNumberOfOlderOnActivityFeed(demoIdentity, baseActivity));
    assertEquals(0, mongoStorage.getOlderOnActivityFeed(demoIdentity, baseActivity, 10).size());
    //
    createActivities(1, johnIdentity);
    assertEquals(0, mongoStorage.getNumberOfOlderOnActivityFeed(demoIdentity, baseActivity));
    assertEquals(0, mongoStorage.getOlderOnActivityFeed(demoIdentity, baseActivity, 10).size());
    //
    baseActivity = demoActivityFeed.get(2);
    assertEquals(2, mongoStorage.getNumberOfOlderOnActivityFeed(demoIdentity, baseActivity));
    assertEquals(2, mongoStorage.getOlderOnActivityFeed(demoIdentity, baseActivity, 10).size());
    
    //clear data
    relationshipManager.delete(maryDemoConnection);
  }
  
  public void testGetNewerOnActivitiesOfConnections() throws Exception {
    List<Relationship> relationships = new ArrayList<Relationship> ();
    createActivities(3, maryIdentity);
    createActivities(1, demoIdentity);
    createActivities(2, johnIdentity);
    createActivities(2, rootIdentity);
    
    List<ExoSocialActivity> maryActivities = mongoStorage.getActivitiesOfIdentity(maryIdentity, 0, 10);
    assertEquals(3, maryActivities.size());
    
    //base activity is the first activity created by mary
    ExoSocialActivity baseActivity = maryActivities.get(2);
    
    //As mary has no connections, there are any activity on her connection stream
    assertEquals(0, mongoStorage.getNewerOnActivitiesOfConnections(maryIdentity, baseActivity, 10).size());
    assertEquals(0, mongoStorage.getNumberOfNewerOnActivitiesOfConnections(maryIdentity, baseActivity));
    
    //demo connect with mary
    Relationship maryDemoRelationship = relationshipManager.inviteToConnect(maryIdentity, demoIdentity);
    relationshipManager.confirm(maryIdentity, demoIdentity);
    relationships.add(maryDemoRelationship);
    
    assertEquals(1, mongoStorage.getNewerOnActivitiesOfConnections(maryIdentity, baseActivity, 10).size());
    assertEquals(1, mongoStorage.getNumberOfNewerOnActivitiesOfConnections(maryIdentity, baseActivity));
    
    //demo has 2 activities created by mary newer than the base activity
    assertEquals(2, mongoStorage.getNewerOnActivitiesOfConnections(demoIdentity, baseActivity, 10).size());
    assertEquals(2, mongoStorage.getNumberOfNewerOnActivitiesOfConnections(demoIdentity, baseActivity));
    
    //john connects with mary
    Relationship maryJohnRelationship = relationshipManager.inviteToConnect(maryIdentity, johnIdentity);
    relationshipManager.confirm(maryIdentity, johnIdentity);
    relationships.add(maryJohnRelationship);
    
    assertEquals(3, mongoStorage.getNewerOnActivitiesOfConnections(maryIdentity, baseActivity, 10).size());
    assertEquals(3, mongoStorage.getNumberOfNewerOnActivitiesOfConnections(maryIdentity, baseActivity));
    
    assertEquals(2, mongoStorage.getNewerOnActivitiesOfConnections(johnIdentity, baseActivity, 10).size());
    assertEquals(2, mongoStorage.getNumberOfNewerOnActivitiesOfConnections(johnIdentity, baseActivity));
    
    //mary connects with root
    Relationship maryRootRelationship = relationshipManager.inviteToConnect(maryIdentity, rootIdentity);
    relationshipManager.confirm(maryIdentity, rootIdentity);
    relationships.add(maryRootRelationship);
    
    assertEquals(5, mongoStorage.getNewerOnActivitiesOfConnections(maryIdentity, baseActivity, 10).size());
    assertEquals(5, mongoStorage.getNumberOfNewerOnActivitiesOfConnections(maryIdentity, baseActivity));
    
    for (Relationship rel : relationships) {
      relationshipManager.delete(rel);
    }
  }
  
  public void testGetOlderOnActivitiesOfConnections() throws Exception {
    List<Relationship> relationships = new ArrayList<Relationship> ();
    createActivities(3, maryIdentity);
    createActivities(1, demoIdentity);
    createActivities(2, johnIdentity);
    createActivities(2, rootIdentity);
    
    List<ExoSocialActivity> maryActivities = mongoStorage.getActivitiesOfIdentity(maryIdentity, 0, 10);
    assertEquals(3, maryActivities.size());
    
    //base activity is the first activity created by mary
    ExoSocialActivity baseActivity = maryActivities.get(2);
    
    //As mary has no connections, there are any activity on her connection stream
    assertEquals(0, mongoStorage.getOlderOnActivitiesOfConnections(maryIdentity, baseActivity, 10).size());
    assertEquals(0, mongoStorage.getNumberOfOlderOnActivitiesOfConnections(maryIdentity, baseActivity));
    
    //demo connect with mary
    Relationship maryDemoRelationship = relationshipManager.inviteToConnect(maryIdentity, demoIdentity);
    relationshipManager.confirm(maryIdentity, demoIdentity);
    relationships.add(maryDemoRelationship);
    
    baseActivity = mongoStorage.getActivitiesOfIdentity(demoIdentity, 0, 10).get(0);
    assertEquals(0, mongoStorage.getOlderOnActivitiesOfConnections(maryIdentity, baseActivity, 10).size());
    assertEquals(0, mongoStorage.getNumberOfOlderOnActivitiesOfConnections(maryIdentity, baseActivity));
    
    assertEquals(3, mongoStorage.getOlderOnActivitiesOfConnections(demoIdentity, baseActivity, 10).size());
    assertEquals(3, mongoStorage.getNumberOfOlderOnActivitiesOfConnections(demoIdentity, baseActivity));
    
    //john connects with mary
    Relationship maryJohnRelationship = relationshipManager.inviteToConnect(maryIdentity, johnIdentity);
    relationshipManager.confirm(maryIdentity, johnIdentity);
    relationships.add(maryJohnRelationship);
    
    baseActivity = mongoStorage.getActivitiesOfIdentity(johnIdentity, 0, 10).get(0);
    assertEquals(2, mongoStorage.getOlderOnActivitiesOfConnections(maryIdentity, baseActivity, 10).size());
    assertEquals(2, mongoStorage.getNumberOfOlderOnActivitiesOfConnections(maryIdentity, baseActivity));
    
    assertEquals(3, mongoStorage.getOlderOnActivitiesOfConnections(johnIdentity, baseActivity, 10).size());
    assertEquals(3, mongoStorage.getNumberOfOlderOnActivitiesOfConnections(johnIdentity, baseActivity));
    
    //mary connects with root
    Relationship maryRootRelationship = relationshipManager.inviteToConnect(maryIdentity, rootIdentity);
    relationshipManager.confirm(maryIdentity, rootIdentity);
    relationships.add(maryRootRelationship);
    
    baseActivity = mongoStorage.getActivitiesOfIdentity(rootIdentity, 0, 10).get(0);
    assertEquals(4, mongoStorage.getOlderOnActivitiesOfConnections(maryIdentity, baseActivity, 10).size());
    assertEquals(4, mongoStorage.getNumberOfOlderOnActivitiesOfConnections(maryIdentity, baseActivity));
    
    for (Relationship rel : relationships) {
      relationshipManager.delete(rel);
    }
  }
  
  public void testGetNewerOnUserSpacesActivities() throws Exception {
    Space space = this.getSpaceInstance(spaceService, 0);
    tearDownSpaceList.add(space);
    Identity spaceIdentity = identityManager.getOrCreateIdentity(SpaceIdentityProvider.NAME, space.getPrettyName(), false);
    
    int totalNumber = 10;
    ExoSocialActivity baseActivity = null;
    //demo posts activities to space
    for (int i = 0; i < totalNumber; i ++) {
      ExoSocialActivity activity = new ExoSocialActivityImpl();
      activity.setTitle("activity title " + i);
      activity.setUserId(demoIdentity.getId());
      mongoStorage.saveActivity(spaceIdentity, activity);
      tearDownActivityList.add(activity);
      if (i == 0) {
        baseActivity = activity;
      }
    }
    
    assertEquals(9, mongoStorage.getNewerOnUserSpacesActivities(demoIdentity, baseActivity, 10).size());
    assertEquals(9, mongoStorage.getNumberOfNewerOnUserSpacesActivities(demoIdentity, baseActivity));
    //
    assertEquals(9, mongoStorage.getNewerOnSpaceActivities(spaceIdentity, baseActivity, 10).size());
    assertEquals(9, mongoStorage.getNumberOfNewerOnSpaceActivities(spaceIdentity, baseActivity));
    
    Space space2 = this.getSpaceInstance(spaceService, 1);
    tearDownSpaceList.add(space2);
    Identity spaceIdentity2 = identityManager.getOrCreateIdentity(SpaceIdentityProvider.NAME, space2.getPrettyName(), false);
    //demo posts activities to space2
    for (int i = 0; i < totalNumber; i ++) {
      ExoSocialActivity activity = new ExoSocialActivityImpl();
      activity.setTitle("activity title " + i);
      activity.setUserId(demoIdentity.getId());
      mongoStorage.saveActivity(spaceIdentity2, activity);
      tearDownActivityList.add(activity);
    }
    
    assertEquals(19, mongoStorage.getNewerOnUserSpacesActivities(demoIdentity, baseActivity, 20).size());
    assertEquals(19, mongoStorage.getNumberOfNewerOnUserSpacesActivities(demoIdentity, baseActivity));
  }
  
  public void testGetOlderOnUserSpacesActivities() throws Exception {
    Space space = this.getSpaceInstance(spaceService, 0);
    tearDownSpaceList.add(space);
    Identity spaceIdentity = identityManager.getOrCreateIdentity(SpaceIdentityProvider.NAME, space.getPrettyName(), false);
    
    int totalNumber = 5;
    ExoSocialActivity baseActivity = null;
    //demo posts activities to space
    for (int i = 0; i < totalNumber; i ++) {
      ExoSocialActivity activity = new ExoSocialActivityImpl();
      activity.setTitle("activity title " + i);
      activity.setUserId(demoIdentity.getId());
      mongoStorage.saveActivity(spaceIdentity, activity);
      tearDownActivityList.add(activity);
      if (i == 4) {
        baseActivity = activity;
      }
    }
    
    assertEquals(4, mongoStorage.getOlderOnUserSpacesActivities(demoIdentity, baseActivity, 10).size());
    assertEquals(4, mongoStorage.getNumberOfOlderOnUserSpacesActivities(demoIdentity, baseActivity));
    //
    assertEquals(4, mongoStorage.getOlderOnSpaceActivities(spaceIdentity, baseActivity, 10).size());
    assertEquals(4, mongoStorage.getNumberOfOlderOnSpaceActivities(spaceIdentity, baseActivity));
    
    Space space2 = this.getSpaceInstance(spaceService, 1);
    tearDownSpaceList.add(space2);
    Identity spaceIdentity2 = identityManager.getOrCreateIdentity(SpaceIdentityProvider.NAME, space2.getPrettyName(), false);
    //demo posts activities to space2
    for (int i = 0; i < totalNumber; i ++) {
      ExoSocialActivity activity = new ExoSocialActivityImpl();
      activity.setTitle("activity title " + i);
      activity.setUserId(demoIdentity.getId());
      mongoStorage.saveActivity(spaceIdentity2, activity);
      tearDownActivityList.add(activity);
    }
    
    assertEquals(4, mongoStorage.getOlderOnUserSpacesActivities(demoIdentity, baseActivity, 10).size());
    assertEquals(4, mongoStorage.getNumberOfOlderOnUserSpacesActivities(demoIdentity, baseActivity));
  }

  public void testGetNewerOnUserActivities() {
    createActivities(2, demoIdentity);
    ExoSocialActivity firstActivity = mongoStorage.getUserActivities(demoIdentity, 0, 10).get(0);
    assertEquals(0, mongoStorage.getNewerOnUserActivities(demoIdentity, firstActivity, 10).size());
    assertEquals(0, mongoStorage.getNumberOfNewerOnUserActivities(demoIdentity, firstActivity));
    //
    createActivities(2, maryIdentity);
    assertEquals(0, mongoStorage.getNewerOnUserActivities(demoIdentity, firstActivity, 10).size());
    assertEquals(0, mongoStorage.getNumberOfNewerOnUserActivities(demoIdentity, firstActivity));
    //
    createActivities(2, demoIdentity);
    assertEquals(2, mongoStorage.getNewerOnUserActivities(demoIdentity, firstActivity, 10).size());
    assertEquals(2, mongoStorage.getNumberOfNewerOnUserActivities(demoIdentity, firstActivity));
  }
  
  public void testGetOlderOnUserActivities() {
    createActivities(2, demoIdentity);
    ExoSocialActivity baseActivity = mongoStorage.getUserActivities(demoIdentity, 0, 10).get(0);
    assertEquals(1, mongoStorage.getOlderOnUserActivities(demoIdentity, baseActivity, 10).size());
    assertEquals(1, mongoStorage.getNumberOfOlderOnUserActivities(demoIdentity, baseActivity));
    //
    createActivities(2, maryIdentity);
    assertEquals(1, mongoStorage.getOlderOnUserActivities(demoIdentity, baseActivity, 10).size());
    assertEquals(1, mongoStorage.getNumberOfOlderOnUserActivities(demoIdentity, baseActivity));
    //
    createActivities(2, demoIdentity);
    baseActivity = mongoStorage.getUserActivities(demoIdentity, 0, 10).get(0);
    assertEquals(3, mongoStorage.getOlderOnUserActivities(demoIdentity, baseActivity, 10).size());
    assertEquals(3, mongoStorage.getNumberOfOlderOnUserActivities(demoIdentity, baseActivity));
  }
  
  public void testGetNewerComments() {
    int totalNumber = 10;
    
    ExoSocialActivity activity = new ExoSocialActivityImpl();
    activity.setTitle("activity title");
    activity.setUserId(rootIdentity.getId());
    mongoStorage.saveActivity(rootIdentity, activity);
    tearDownActivityList.add(activity);
    
    for (int i = 0; i < totalNumber; i ++) {
      //John comments on Root's activity
      ExoSocialActivity comment = new ExoSocialActivityImpl();
      comment.setTitle("john comment " + i);
      comment.setUserId(johnIdentity.getId());
      mongoStorage.saveComment(activity, comment);
    }
    
    for (int i = 0; i < totalNumber; i ++) {
      //John comments on Root's activity
      ExoSocialActivity comment = new ExoSocialActivityImpl();
      comment.setTitle("demo comment " + i);
      comment.setUserId(demoIdentity.getId());
      mongoStorage.saveComment(activity, comment);
    }
    
    List<ExoSocialActivity> comments = mongoStorage.getComments(activity, 0, 20);
    assertEquals(20, comments.size());
    
    ExoSocialActivity baseComment = comments.get(0);
    
    assertEquals(19, mongoStorage.getNewerComments(activity, baseComment, 20).size());
    assertEquals(19, mongoStorage.getNumberOfNewerComments(activity, baseComment));
    
    baseComment = comments.get(9);
    assertEquals(10, mongoStorage.getNewerComments(activity, baseComment, 20).size());
    assertEquals(10, mongoStorage.getNumberOfNewerComments(activity, baseComment));
    
    baseComment = comments.get(19);
    assertEquals(0, mongoStorage.getNewerComments(activity, baseComment, 20).size());
    assertEquals(0, mongoStorage.getNumberOfNewerComments(activity, baseComment));
  }
  
  public void testGetOlderComments() {
    int totalNumber = 10;
    
    ExoSocialActivity activity = new ExoSocialActivityImpl();
    activity.setTitle("activity title");
    activity.setUserId(rootIdentity.getId());
    mongoStorage.saveActivity(rootIdentity, activity);
    tearDownActivityList.add(activity);
    
    for (int i = 0; i < totalNumber; i ++) {
      //John comments on Root's activity
      ExoSocialActivity comment = new ExoSocialActivityImpl();
      comment.setTitle("john comment " + i);
      comment.setUserId(johnIdentity.getId());
      mongoStorage.saveComment(activity, comment);
    }
    
    for (int i = 0; i < totalNumber; i ++) {
      //John comments on Root's activity
      ExoSocialActivity comment = new ExoSocialActivityImpl();
      comment.setTitle("demo comment " + i);
      comment.setUserId(demoIdentity.getId());
      mongoStorage.saveComment(activity, comment);
    }
    
    List<ExoSocialActivity> comments = mongoStorage.getComments(activity, 0, 20);
    assertEquals(20, comments.size());
    
    ExoSocialActivity baseComment = comments.get(19);
    
    assertEquals(19, mongoStorage.getOlderComments(activity, baseComment, 20).size());
    assertEquals(19, mongoStorage.getNumberOfOlderComments(activity, baseComment));
    
    baseComment = comments.get(10);
    assertEquals(10, mongoStorage.getOlderComments(activity, baseComment, 20).size());
    assertEquals(10, mongoStorage.getNumberOfOlderComments(activity, baseComment));
    
    baseComment = comments.get(0);
    assertEquals(0, mongoStorage.getOlderComments(activity, baseComment, 20).size());
    assertEquals(0, mongoStorage.getNumberOfOlderComments(activity, baseComment));
  }
  
  private void createActivities(int number, Identity owner) {
    for (int i = 0; i < number; i++) {
      ExoSocialActivity activity = new ExoSocialActivityImpl();
      activity.setTitle("activity title " + i);
      mongoStorage.saveActivity(owner, activity);
      tearDownActivityList.add(activity);
    }
  }
  
  private Space getSpaceInstance(SpaceService spaceService, int number) throws Exception {
    Space space = new Space();
    space.setDisplayName("my space " + number);
    space.setPrettyName(space.getDisplayName());
    space.setRegistration(Space.OPEN);
    space.setDescription("add new space " + number);
    space.setType(DefaultSpaceApplicationHandler.NAME);
    space.setVisibility(Space.PUBLIC);
    space.setRegistration(Space.VALIDATION);
    space.setPriority(Space.INTERMEDIATE_PRIORITY);
    space.setGroupId("/space/space" + number);
    space.setUrl(space.getPrettyName());
    String[] managers = new String[] {"demo"};
    String[] members = new String[] {"demo"};
    space.setManagers(managers);
    space.setMembers(members);
    spaceService.saveSpace(space, true);
    return space;
  }
  
  private ExoSocialActivity createActivity(int num) {
    //
    ExoSocialActivity activity = new ExoSocialActivityImpl();
    activity.setTitle("Activity "+ num);
    activity.setTitleId("TitleID: "+ activity.getTitle());
    activity.setType("UserActivity");
    activity.setBody("Body of "+ activity.getTitle());
    activity.setBodyId("BodyId of "+ activity.getTitle());
    activity.setLikeIdentityIds(new String[]{"demo", "mary"});
    activity.setMentionedIds(new String[]{"demo", "john"});
    activity.setAppId("AppID");
    activity.setExternalId("External ID");
    
    return activity;
  }
  

}
