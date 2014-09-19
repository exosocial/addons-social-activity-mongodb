/*
 * Copyright (C) 2003-2010 eXo Platform SAS.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see<http://www.gnu.org/licenses/>.
 */
package org.exoplatform.social.core.storage.impl;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

import org.exoplatform.commons.utils.ListAccess;
import org.exoplatform.container.PortalContainer;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;
import org.exoplatform.social.common.RealtimeListAccess;
import org.exoplatform.social.core.activity.model.ExoSocialActivity;
import org.exoplatform.social.core.application.SpaceActivityPublisher;
import org.exoplatform.social.core.identity.model.Identity;
import org.exoplatform.social.core.identity.model.Profile;
import org.exoplatform.social.core.identity.provider.OrganizationIdentityProvider;
import org.exoplatform.social.core.identity.provider.SpaceIdentityProvider;
import org.exoplatform.social.core.manager.ActivityManager;
import org.exoplatform.social.core.manager.IdentityManager;
import org.exoplatform.social.core.manager.RelationshipManager;
import org.exoplatform.social.core.model.AvatarAttachment;
import org.exoplatform.social.core.mongo.storage.MongoStorage;
import org.exoplatform.social.core.relationship.model.Relationship;
import org.exoplatform.social.core.space.impl.DefaultSpaceApplicationHandler;
import org.exoplatform.social.core.space.model.Space;
import org.exoplatform.social.core.space.model.Space.UpdatedField;
import org.exoplatform.social.core.space.spi.SpaceLifeCycleEvent;
import org.exoplatform.social.core.space.spi.SpaceService;
import org.exoplatform.social.core.storage.api.IdentityStorage;
import org.exoplatform.social.core.storage.api.SpaceStorage;
import org.exoplatform.social.core.test.AbstractCoreTest;

/**
 * Unit Tests for {@link SpaceActivityPublisher}
 * @author hoat_le
 */
public class SpaceActivityMongoDBPublisherTest extends  AbstractCoreTest {
  private final Log LOG = ExoLogger.getLogger(SpaceActivityMongoDBPublisherTest.class);
  private ActivityManager activityManager;
  private IdentityManager identityManager;
  private RelationshipManager relationshipManager;
  private IdentityStorage identityStorage;
  private SpaceService spaceService;
  private SpaceStorage spaceStorage;
  private SpaceActivityPublisher spaceActivityPublisher;
  private List<Identity> tearDownIdentityList;
  private List<Space> tearDownSpaceList;
  @Override
  public void setUp() throws Exception {
    super.setUp();
    tearDownIdentityList = new ArrayList<Identity>();
    tearDownSpaceList = new ArrayList<Space>();
    activityManager = (ActivityManager) getContainer().getComponentInstanceOfType(ActivityManager.class);
    assertNotNull("activityManager must not be null", activityManager);
    identityManager =  (IdentityManager) getContainer().getComponentInstanceOfType(IdentityManager.class);
    assertNotNull("identityManager must not be null", identityManager);
    spaceService = (SpaceService) getContainer().getComponentInstanceOfType(SpaceService.class);
    assertNotNull("spaceService must not be null", spaceService);
    spaceStorage = (SpaceStorage) getContainer().getComponentInstanceOfType(SpaceStorage.class);
    assertNotNull(spaceStorage);
    spaceActivityPublisher = (SpaceActivityPublisher) getContainer().getComponentInstanceOfType(SpaceActivityPublisher.class);
    assertNotNull("spaceActivityPublisher must not be null", spaceActivityPublisher);
    identityStorage =  (IdentityStorage) getContainer().getComponentInstanceOfType(IdentityStorage.class);
    assertNotNull("identityStorage must not be null", identityStorage);
    relationshipManager = (RelationshipManager) getContainer().getComponentInstanceOfType(RelationshipManager.class);
  }

  @Override
  public void tearDown() throws Exception {
    for (Identity identity : tearDownIdentityList) {
      identityManager.deleteIdentity(identity);
    }
    for (Space space : tearDownSpaceList) {
      Identity spaceIdentity = identityManager.getOrCreateIdentity(SpaceIdentityProvider.NAME, space.getPrettyName(), false);
      if (spaceIdentity != null) {
        identityManager.deleteIdentity(spaceIdentity);
      }
      spaceService.deleteSpace(space);
    }
    super.tearDown();
  }

  /**
   *
   * @throws Exception
   */
  public void testSpaceCreation() throws Exception {
    Identity rootIdentity = identityManager.getOrCreateIdentity(OrganizationIdentityProvider.NAME, "root", true);
    Identity demoIdentity = identityManager.getOrCreateIdentity(OrganizationIdentityProvider.NAME, "demo", true);
    tearDownIdentityList.add(rootIdentity);
    tearDownIdentityList.add(demoIdentity);

    Space space = new Space();
    space.setDisplayName("Toto");
    space.setPrettyName(space.getDisplayName());
    space.setGroupId("/platform/users");
    space.setVisibility(Space.PRIVATE);
    space.setType(DefaultSpaceApplicationHandler.NAME);
    String[] managers = new String[] {"root"};
    String[] members = new String[] {"root"};
    space.setManagers(managers);
    space.setMembers(members);
    spaceService.saveSpace(space, true);
    tearDownSpaceList.add(space);
    
    assertNotNull("space.getId() must not be null", space.getId());
    SpaceLifeCycleEvent event  = new SpaceLifeCycleEvent(space, rootIdentity.getRemoteId(), SpaceLifeCycleEvent.Type.SPACE_CREATED);
    spaceActivityPublisher.spaceCreated(event);

    Thread.sleep(3000);

    Identity spaceIdentity = identityManager.getOrCreateIdentity(SpaceIdentityProvider.NAME, space.getPrettyName(), true);
    RealtimeListAccess<ExoSocialActivity> listAccess = activityManager.getActivitiesOfSpaceWithListAccess(spaceIdentity);
    assertEquals(1, listAccess.getSize());
    assertEquals(1, listAccess.loadAsList(0, 10).size());
    
    Relationship demoRootConnection = relationshipManager.inviteToConnect(demoIdentity, rootIdentity);
    relationshipManager.confirm(rootIdentity, demoIdentity);
    
    List<ExoSocialActivity> activities = activityManager.getActivityFeedWithListAccess(demoIdentity).loadAsList(0, 10);
    assertEquals(0, activities.size());

    //add demo to the space
    spaceService.addMember(space, demoIdentity.getRemoteId());
    
    listAccess = activityManager.getActivitiesOfSpaceWithListAccess(spaceIdentity);
    assertEquals(1, listAccess.getSize());
    assertEquals(1, listAccess.loadAsList(0, 10).size());
    
    //
    relationshipManager.delete(demoRootConnection);
  }
  
  /**
  *
  * @throws Exception
  */
 public void testSpaceUpdated() throws Exception {
   Identity rootIdentity = identityManager.getOrCreateIdentity(OrganizationIdentityProvider.NAME, "root", false);
   Identity demoIdentity = identityManager.getOrCreateIdentity(OrganizationIdentityProvider.NAME, "demo", false);
   tearDownIdentityList.add(rootIdentity);
   tearDownIdentityList.add(demoIdentity);

   Space space = new Space();
   space.setDisplayName("Toto");
   space.setPrettyName(space.getDisplayName());
   space.setGroupId("/platform/users");
   space.setVisibility(Space.PRIVATE);
   space.setType(DefaultSpaceApplicationHandler.NAME);
   String[] managers = new String[] {"root"};
   String[] members = new String[] {"root"};
   space.setManagers(managers);
   space.setMembers(members);
   spaceService.saveSpace(space, true);
   tearDownSpaceList.add(space);
   
   assertNotNull("space.getId() must not be null", space.getId());
   SpaceLifeCycleEvent event  = new SpaceLifeCycleEvent(space, rootIdentity.getRemoteId(), SpaceLifeCycleEvent.Type.SPACE_CREATED);
   spaceActivityPublisher.spaceCreated(event);

   Identity spaceIdentity = identityManager.getOrCreateIdentity(SpaceIdentityProvider.NAME, space.getPrettyName(), false);
   String activityId = identityStorage.getProfileActivityId(spaceIdentity.getProfile(), Profile.AttachedActivityType.SPACE);
   ExoSocialActivity activity = activityManager.getActivity(activityId);
   List<ExoSocialActivity> comments = activityManager.getCommentsWithListAccess(activity).loadAsList(0, 20);
   //Number of comments must be 1
   assertEquals(1, comments.size());
   assertEquals("Has joined the space.", comments.get(0).getTitle());
   
   //Rename space
   space.setEditor(rootIdentity.getRemoteId());
   spaceService.renameSpace(space, "Social");
   activity = activityManager.getActivity(activityId);
   assertEquals(2, activityManager.getCommentsWithListAccess(activity).getSize());
   comments = activityManager.getCommentsWithListAccess(activity).loadAsList(0, 20);
   assertEquals("Name has been updated to: "+space.getDisplayName(), comments.get(1).getTitle());
   
   //Update space's description
   space.setDescription("social's team");
   space.setField(UpdatedField.DESCRIPTION);
   spaceService.updateSpace(space);
   comments = activityManager.getCommentsWithListAccess(activity).loadAsList(0, 20);
   assertEquals(3, comments.size());
   assertEquals("Description has been updated to: "+space.getDescription(), comments.get(2).getTitle());
   
   //update avatar
   AvatarAttachment avatar = new AvatarAttachment();
   avatar.setMimeType("plain/text");
   avatar.setInputStream(new ByteArrayInputStream("Attachment content".getBytes()));
   space.setAvatarAttachment(avatar);
   spaceService.updateSpaceAvatar(space);
   comments = activityManager.getCommentsWithListAccess(activity).loadAsList(0, 20);
   assertEquals(4, comments.size());
   assertEquals("Space has a new avatar.", comments.get(3).getTitle());

   // delete this activity
   activityManager.deleteActivity(activityId);
   assertEquals(0, activityManager.getActivitiesWithListAccess(spaceIdentity).getSize());
   
   space.setField(null);
   spaceService.renameSpace(space, "SocialTeam");
   activityId = identityStorage.getProfileActivityId(spaceIdentity.getProfile(), Profile.AttachedActivityType.SPACE);
   ExoSocialActivity newActivity = activityManager.getActivity(activityId);
   //Number of comments must be 1
   assertEquals(1, activityManager.getCommentsWithListAccess(newActivity).getSize());
   
   { // test case for grant or revoke manage role of users
     String[] spaceManagers = new String[] {"root"};
     String[] spaceMembers = new String[] {"demo"};
     space.setField(null);
     space.setManagers(spaceManagers);
     space.setMembers(spaceMembers);
     space.setEditor("root");
     
     spaceService.setManager(space, "demo", true);
     
     comments = activityManager.getCommentsWithListAccess(newActivity).loadAsList(0, 20);
     
     assertEquals(2, activityManager.getCommentsWithListAccess(newActivity).getSize());
     assertEquals(identityManager.getOrCreateIdentity(OrganizationIdentityProvider.NAME, "root", false).getId(), comments.get(1).getUserId());
     
     //
     spaceService.setManager(space, "demo", false);
     
     comments = activityManager.getCommentsWithListAccess(newActivity).loadAsList(0, 20);
     
     assertEquals(3, activityManager.getCommentsWithListAccess(newActivity).getSize());
     assertEquals(identityManager.getOrCreateIdentity(OrganizationIdentityProvider.NAME, "root", false).getId(), comments.get(2).getUserId());
   }
   
   {// update both name and description
     assertEquals("social's team", space.getDescription());
     
     String newDescription = "new description";
     space.setDescription(newDescription);
     space.setField(UpdatedField.DESCRIPTION);
     String newDisplayName = "newSpaceName";
     spaceService.renameSpace(space, newDisplayName);
     comments = activityManager.getCommentsWithListAccess(newActivity).loadAsList(0, 20);
     assertEquals(5, comments.size());
     assertEquals("Name has been updated to: " + space.getDisplayName(), comments.get(3).getTitle());
     assertEquals("Description has been updated to: " + space.getDescription(), comments.get(4).getTitle());
   }
   
   {
     assertEquals("new description", space.getDescription());
     
     space.setDescription("Cet espace est à chercher des bugs");
     space.setField(UpdatedField.DESCRIPTION);
     spaceService.updateSpace(space);
     comments = activityManager.getCommentsWithListAccess(newActivity).loadAsList(0, 20);
     assertEquals(6, comments.size());
   }
   
 }
 
 public void testSpaceHidden() throws Exception {
   Identity rootIdentity = identityManager.getOrCreateIdentity(OrganizationIdentityProvider.NAME, "root", true);
   tearDownIdentityList.add(rootIdentity);

   //Create a hidden space
   Space space = new Space();
   space.setDisplayName("Toto");
   space.setPrettyName(space.getDisplayName());
   space.setGroupId("/platform/users");
   space.setVisibility(Space.PRIVATE);
   String[] managers = new String[] {"root"};
   String[] members = new String[] {"root"};
   space.setManagers(managers);
   space.setMembers(members);
   spaceService.saveSpace(space, true);
   tearDownSpaceList.add(space);
   
   //broadcast event
   SpaceLifeCycleEvent event  = new SpaceLifeCycleEvent(space, rootIdentity.getRemoteId(), SpaceLifeCycleEvent.Type.SPACE_CREATED);
   spaceActivityPublisher.spaceCreated(event);

   Identity spaceIdentity = identityManager.getOrCreateIdentity(SpaceIdentityProvider.NAME, space.getPrettyName(), false);
   ListAccess<ExoSocialActivity> spaceActivities = activityManager.getActivitiesOfSpaceWithListAccess(spaceIdentity);
   ListAccess<ExoSocialActivity> userActivities = activityManager.getActivitiesWithListAccess(rootIdentity);
   ListAccess<ExoSocialActivity> userFeedActivities = activityManager.getActivityFeedWithListAccess(rootIdentity);
   
   assertEquals(1, userFeedActivities.getSize());
   assertEquals(1, userFeedActivities.load(0, 10).length);
   assertEquals(1, userActivities.getSize());
   assertEquals(1, userActivities.load(0, 10).length);
   assertEquals(1, spaceActivities.getSize());
   assertEquals(1, spaceActivities.load(0, 10).length);
   
   //Set space's visibility to PRIVATE
   space.setVisibility(Space.HIDDEN);
   spaceService.saveSpace(space, false);
   
   spaceActivities = activityManager.getActivitiesOfSpaceWithListAccess(spaceIdentity);
   userActivities = activityManager.getActivitiesWithListAccess(rootIdentity);
   userFeedActivities = activityManager.getActivityFeedWithListAccess(rootIdentity);
   
   //Check space activity stream
   assertEquals(0, spaceActivities.getSize());
   assertEquals(0, spaceActivities.load(0, 10).length);
   
   //Check user activity stream
   assertEquals(0, userActivities.getSize());
   assertEquals(0, userActivities.load(0, 10).length);
   
   //Check user feed activity stream
   assertEquals(0, userFeedActivities.getSize());
   assertEquals(0, userFeedActivities.load(0, 10).length);
   
   //Set space's visibility to PRIVATE
   space.setVisibility(Space.PRIVATE);
   spaceService.saveSpace(space, false);
   
   spaceActivities = activityManager.getActivitiesOfSpaceWithListAccess(spaceIdentity);
   userActivities = activityManager.getActivitiesWithListAccess(rootIdentity);
   userFeedActivities = activityManager.getActivityFeedWithListAccess(rootIdentity);
   
   //Check space activity stream
   assertEquals(1, spaceActivities.getSize());
   assertEquals(1, spaceActivities.load(0, 10).length);
   
   //Check user activity stream
   assertEquals(1, userActivities.getSize());
   assertEquals(1, userActivities.load(0, 10).length);
   
   //Check user feed activity stream
   assertEquals(1, userFeedActivities.getSize());
   assertEquals(1, userFeedActivities.load(0, 10).length);

 }

}
