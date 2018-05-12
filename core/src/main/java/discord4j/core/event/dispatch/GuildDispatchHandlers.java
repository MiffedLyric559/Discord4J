/*
 * This file is part of Discord4J.
 *
 * Discord4J is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Discord4J is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with Discord4J.  If not, see <http://www.gnu.org/licenses/>.
 */
package discord4j.core.event.dispatch;

import discord4j.common.json.GuildEmojiResponse;
import discord4j.common.json.GuildMemberResponse;
import discord4j.common.json.UserResponse;
import discord4j.core.DiscordClient;
import discord4j.core.ServiceMediator;
import discord4j.core.StateHolder;
import discord4j.core.event.domain.guild.*;
import discord4j.core.event.domain.role.RoleCreateEvent;
import discord4j.core.event.domain.role.RoleDeleteEvent;
import discord4j.core.event.domain.role.RoleUpdateEvent;
import discord4j.core.object.data.stored.*;
import discord4j.core.object.entity.*;
import discord4j.core.object.util.Snowflake;
import discord4j.core.util.ArrayUtil;
import discord4j.core.util.EntityUtil;
import discord4j.gateway.json.GatewayPayload;
import discord4j.gateway.json.RequestGuildMembers;
import discord4j.gateway.json.dispatch.*;
import discord4j.store.api.util.LongLongTuple2;
import discord4j.store.api.util.LongObjTuple2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

class GuildDispatchHandlers {

    private final boolean readOnly;

    GuildDispatchHandlers(boolean readOnly) {
        this.readOnly = readOnly;
    }

    private <T> Mono<T> emptyIfReadOnly(Mono<T> mono) {
        return readOnly ? Mono.empty() : mono;
    }

    Mono<BanEvent> guildBanAdd(DispatchContext<GuildBanAdd> context) {
        User user = new User(context.getServiceMediator(), new UserBean(context.getDispatch().getUser()));
        long guildId = context.getDispatch().getGuildId();

        return Mono.just(new BanEvent(context.getServiceMediator().getClient(), user, guildId));
    }

    Mono<UnbanEvent> guildBanRemove(DispatchContext<GuildBanRemove> context) {
        User user = new User(context.getServiceMediator(), new UserBean(context.getDispatch().getUser()));
        long guildId = context.getDispatch().getGuildId();

        return Mono.just(new UnbanEvent(context.getServiceMediator().getClient(), user, guildId));
    }

    Mono<GuildCreateEvent> guildCreate(DispatchContext<GuildCreate> context) {
        ServiceMediator serviceMediator = context.getServiceMediator();

        GuildBean guildBean = new GuildBean(context.getDispatch());
        if (guildBean.getLarge()) {
            // Solves https://github.com/Discord4J/Discord4J/issues/429
            // Member store cannot have duplicates because keys cannot
            // be duplicated, but array addition in GuildBean can
            guildBean.setMembers(new long[0]);
        }

        Mono<Void> saveGuild = emptyIfReadOnly(serviceMediator.getStateHolder().getGuildStore()
                .save(guildBean.getId(), guildBean));
        // TODO optimize to separate into three Publisher<Channel> and saveAll to limit store hits
        Mono<Void> saveChannels = Flux.just(context.getDispatch().getChannels()).flatMap(channel -> {
            switch (Channel.Type.of(channel.getType())) {
                case GUILD_TEXT:
                    TextChannelBean textChannelBean = new TextChannelBean(channel, guildBean.getId());
                    textChannelBean.setGuildId(guildBean.getId());
                    return emptyIfReadOnly(serviceMediator.getStateHolder().getTextChannelStore()
                            .save(channel.getId(), textChannelBean));
                case GUILD_VOICE:
                    VoiceChannelBean voiceChannelBean = new VoiceChannelBean(channel, guildBean.getId());
                    voiceChannelBean.setGuildId(guildBean.getId());
                    return emptyIfReadOnly(serviceMediator.getStateHolder().getVoiceChannelStore()
                            .save(channel.getId(), voiceChannelBean));
                case GUILD_CATEGORY:
                    CategoryBean categoryBean = new CategoryBean(channel, guildBean.getId());
                    categoryBean.setGuildId(guildBean.getId());
                    return emptyIfReadOnly(serviceMediator.getStateHolder().getCategoryStore()
                            .save(channel.getId(), categoryBean));
                default:
                    return EntityUtil.throwUnsupportedDiscordValue(channel.getType());
            }
        }).then();

        Mono<Void> saveRoles = emptyIfReadOnly(serviceMediator.getStateHolder().getRoleStore()
                .save(Flux.just(context.getDispatch().getRoles())
                        .map(role -> Tuples.of(role.getId(), new RoleBean(role)))));

        Mono<Void> saveEmojis = emptyIfReadOnly(serviceMediator.getStateHolder().getGuildEmojiStore()
                .save(Flux.just(context.getDispatch().getEmojis())
                        .map(emoji -> Tuples.of(emoji.getId(), new GuildEmojiBean(emoji)))));

        Mono<Void> saveMembers = emptyIfReadOnly(serviceMediator.getStateHolder().getMemberStore()
                .save(Flux.just(context.getDispatch().getMembers())
                        .map(member -> Tuples.of(LongLongTuple2.of(guildBean.getId(), member.getUser().getId()),
                                new MemberBean(member)))));

        Mono<Void> saveUsers = emptyIfReadOnly(serviceMediator.getStateHolder().getUserStore()
                .save(Flux.just(context.getDispatch().getMembers())
                        .map(GuildMemberResponse::getUser)
                        .map(UserBean::new)
                        .map(bean -> Tuples.of(bean.getId(), bean))));

        Mono<Void> saveVoiceStates = emptyIfReadOnly(serviceMediator.getStateHolder().getVoiceStateStore()
                .save(Flux.just(context.getDispatch().getVoiceStates())
                        .map(voiceState -> Tuples.of(LongLongTuple2.of(guildBean.getId(), voiceState.getUserId()),
                                new VoiceStateBean(voiceState, guildBean.getId())))));

        Mono<Void> savePresences = emptyIfReadOnly(serviceMediator.getStateHolder().getPresenceStore()
                .save(Flux.just(context.getDispatch().getPresences())
                        .map(presence -> Tuples.of(LongLongTuple2.of(guildBean.getId(), presence.getUser().getId()),
                                new PresenceBean(presence)))));

        Mono<Void> startMemberChunk = emptyIfReadOnly(Mono.just(guildBean)
                .filter(GuildBean::getLarge)
                .doOnNext(bean -> context.getServiceMediator().getGatewayClient().sender()
                        .next(GatewayPayload.requestGuildMembers(new RequestGuildMembers(bean.getId(), "", 0))))
                .then());

        Mono<Void> saveOfflinePresences = emptyIfReadOnly(Mono.just(guildBean.getMembers())
                .map(LongStream::of)
                .map(LongStream::boxed)
                .flatMapMany(Flux::fromStream)
                .filterWhen(id -> serviceMediator.getStateHolder().getPresenceStore()
                        .find(LongLongTuple2.of(guildBean.getId(), id))
                        .hasElement()
                        .map(identity -> !identity))
                .flatMap(id -> serviceMediator.getStateHolder().getPresenceStore()
                        .save(LongLongTuple2.of(guildBean.getId(), id), PresenceBean.DEFAULT_OFFLINE))
                .then());

        return saveGuild
                .and(saveChannels)
                .and(saveRoles)
                .and(saveEmojis)
                .and(saveMembers)
                .and(saveUsers)
                .and(saveVoiceStates)
                .and(savePresences)
                .and(saveOfflinePresences)
                .and(startMemberChunk)
                .thenReturn(new GuildCreateEvent(serviceMediator.getClient(), new Guild(serviceMediator, guildBean)));
    }

    Mono<GuildDeleteEvent> guildDelete(DispatchContext<GuildDelete> context) {
        DiscordClient client = context.getServiceMediator().getClient();
        StateHolder stateHolder = context.getServiceMediator().getStateHolder();

        long guildId = context.getDispatch().getGuild().getId();
        boolean unavailable = context.getDispatch().getGuild().isUnavailable();

        Mono<Void> deleteGuild = emptyIfReadOnly(stateHolder.getGuildStore().delete(guildId));

        return stateHolder.getGuildStore()
                .find(context.getDispatch().getGuild().getId())
                .flatMap(guild -> {
                    Flux<Long> channels = Flux.fromStream(() -> LongStream.of(guild.getChannels()).boxed());
                    Flux<Long> roles = Flux.fromStream(() -> LongStream.of(guild.getRoles()).boxed());
                    Flux<Long> emojis = Flux.fromStream(() -> LongStream.of(guild.getEmojis()).boxed());

                    Mono<Void> deleteTextChannels = emptyIfReadOnly(stateHolder.getTextChannelStore().delete(channels));
                    Mono<Void> deleteVoiceChannels =
                            emptyIfReadOnly(stateHolder.getVoiceChannelStore().delete(channels));
                    Mono<Void> deleteCategories = emptyIfReadOnly(stateHolder.getCategoryStore().delete(channels));
                    Mono<Void> deleteRoles = emptyIfReadOnly(stateHolder.getRoleStore().delete(roles));
                    Mono<Void> deleteEmojis = emptyIfReadOnly(stateHolder.getGuildEmojiStore().delete(emojis));
                    Mono<Void> deleteMembers = stateHolder.getMemberStore()
                            .deleteInRange(LongLongTuple2.of(guild.getId(), 0), LongLongTuple2.of(guild.getId(), -1));
                    // TODO delete messages
                    // TODO delete no longer visible users
                    Mono<Void> deleteVoiceStates = stateHolder.getVoiceStateStore()
                            .deleteInRange(LongLongTuple2.of(guild.getId(), 0), LongLongTuple2.of(guild.getId(), -1));
                    Mono<Void> deletePresences = stateHolder.getPresenceStore()
                            .deleteInRange(LongLongTuple2.of(guild.getId(), 0), LongLongTuple2.of(guild.getId(), -1));

                    return deleteTextChannels
                            .and(deleteVoiceChannels)
                            .and(deleteCategories)
                            .and(deleteRoles)
                            .and(deleteEmojis)
                            .and(deleteMembers)
                            .and(deleteVoiceStates)
                            .and(deletePresences)
                            .thenReturn(guild);
                })
                .flatMap(deleteGuild::thenReturn)
                .map(bean -> {
                    Guild guild = new Guild(context.getServiceMediator(), bean);
                    return new GuildDeleteEvent(client, guildId, guild, unavailable);
                })
                .switchIfEmpty(deleteGuild.thenReturn(new GuildDeleteEvent(client, guildId, null, unavailable)));
    }

    Mono<EmojisUpdateEvent> guildEmojisUpdate(DispatchContext<GuildEmojisUpdate> context) {
        ServiceMediator serviceMediator = context.getServiceMediator();

        Mono<Void> updateGuildBean = serviceMediator.getStateHolder().getGuildStore()
                .find(context.getDispatch().getGuildId())
                .doOnNext(guild -> {
                    long[] emojis = Arrays.stream(context.getDispatch().getEmojis())
                            .mapToLong(GuildEmojiResponse::getId)
                            .toArray();

                    guild.setEmojis(emojis);
                })
                .flatMap(guild -> emptyIfReadOnly(serviceMediator.getStateHolder().getGuildStore()
                        .save(guild.getId(), guild)));

        Mono<Void> saveEmojis = serviceMediator.getStateHolder().getGuildEmojiStore()
                .saveWithLong(Flux.fromArray(context.getDispatch().getEmojis())
                        .map(GuildEmojiBean::new)
                        .map(bean -> LongObjTuple2.of(bean.getId(), bean)));

        DiscordClient client = context.getServiceMediator().getClient();
        long guildId = context.getDispatch().getGuildId();

        Set<GuildEmoji> emojis = Arrays.stream(context.getDispatch().getEmojis())
                .map(GuildEmojiBean::new)
                .map(bean -> new GuildEmoji(serviceMediator, bean, guildId))
                .collect(Collectors.toSet());

        return updateGuildBean
                .and(saveEmojis)
                .thenReturn(new EmojisUpdateEvent(client, guildId, emojis));
    }

    Mono<IntegrationsUpdateEvent> guildIntegrationsUpdate(DispatchContext<GuildIntegrationsUpdate> context) {
        return Mono.just(new IntegrationsUpdateEvent(context.getServiceMediator().getClient(),
                context.getDispatch().getGuildId()));
    }

    Mono<MemberJoinEvent> guildMemberAdd(DispatchContext<GuildMemberAdd> context) {
        ServiceMediator serviceMediator = context.getServiceMediator();
        long guildId = context.getDispatch().getGuildId();
        GuildMemberResponse response = context.getDispatch().getMember();
        MemberBean bean = new MemberBean(response);
        UserBean userBean = new UserBean(response.getUser());

        Mono<Void> addMemberId = serviceMediator.getStateHolder().getGuildStore()
                .find(guildId)
                .doOnNext(guild -> guild.setMembers(ArrayUtil.add(guild.getMembers(), response.getUser().getId())))
                .flatMap(guild -> emptyIfReadOnly(serviceMediator.getStateHolder().getGuildStore()
                        .save(guildId, guild)));

        Mono<Void> saveMember = emptyIfReadOnly(serviceMediator.getStateHolder().getMemberStore()
                .save(LongLongTuple2.of(guildId, response.getUser().getId()), bean));

        Mono<Void> saveUser = emptyIfReadOnly(serviceMediator.getStateHolder().getUserStore()
                .save(response.getUser().getId(), userBean));

        Member member = new Member(serviceMediator, bean, userBean, guildId);

        return addMemberId
                .and(saveMember)
                .and(saveUser)
                .thenReturn(new MemberJoinEvent(serviceMediator.getClient(), member, guildId));
    }

    Mono<MemberLeaveEvent> guildMemberRemove(DispatchContext<GuildMemberRemove> context) {
        ServiceMediator serviceMediator = context.getServiceMediator();
        long guildId = context.getDispatch().getGuildId();
        UserResponse response = context.getDispatch().getUser();

        Mono<Void> removeMemberId = serviceMediator.getStateHolder().getGuildStore()
                .find(guildId)
                .doOnNext(guild -> guild.setMembers(ArrayUtil.remove(guild.getMembers(), response.getId())))
                .flatMap(guild -> emptyIfReadOnly(serviceMediator.getStateHolder().getGuildStore()
                        .save(guildId, guild)));

        Mono<Member> member = serviceMediator.getStateHolder().getMemberStore()
                .find(LongLongTuple2.of(guildId, response.getId()))
                .map(bean -> new Member(serviceMediator, bean, new UserBean(response), guildId));

        Mono<Void> deleteMember = emptyIfReadOnly(serviceMediator.getStateHolder().getMemberStore()
                .delete(LongLongTuple2.of(guildId, response.getId())));

        User user = new User(serviceMediator, new UserBean(response));

        return member.map(Optional::of)
            .defaultIfEmpty(Optional.empty())
            .flatMap(Mono.when(removeMemberId, deleteMember)::thenReturn)
            .map(m -> new MemberLeaveEvent(serviceMediator.getClient(), user, guildId, m.orElse(null)));
    }

    Mono<MemberChunkEvent> guildMembersChunk(DispatchContext<GuildMembersChunk> context) {
        ServiceMediator serviceMediator = context.getServiceMediator();
        long guildId = context.getDispatch().getGuildId();

        Flux<Tuple2<LongLongTuple2, MemberBean>> memberPairs = Flux.fromArray(context.getDispatch().getMembers())
                .map(response -> Tuples.of(response, new MemberBean(response)))
                .map(tuple -> Tuples.of(LongLongTuple2.of(guildId, tuple.getT1().getUser().getId()), tuple.getT2()));

        Flux<Tuple2<Long, UserBean>> userPairs = Flux.fromStream(() -> Arrays.stream(context.getDispatch().getMembers()))
                .map(response -> new UserBean(response.getUser()))
                .map(bean -> Tuples.of(bean.getId(), bean));

        Mono<Void> addMemberIds = serviceMediator.getStateHolder().getGuildStore()
                .find(guildId)
                .doOnNext(guild -> {
                    long[] ids = Arrays.stream(context.getDispatch().getMembers())
                            .map(GuildMemberResponse::getUser)
                            .mapToLong(UserResponse::getId).toArray();

                    guild.setMembers(ArrayUtil.addAll(guild.getMembers(), ids));
                })
                .flatMap(guild -> emptyIfReadOnly(serviceMediator.getStateHolder().getGuildStore()
                        .save(guildId, guild)));

        Mono<Void> saveMembers = emptyIfReadOnly(serviceMediator.getStateHolder().getMemberStore().save(memberPairs));

        Mono<Void> saveUsers = emptyIfReadOnly(serviceMediator.getStateHolder().getUserStore().save(userPairs));

        Set<Member> members = Arrays.stream(context.getDispatch().getMembers())
                .map(response -> Tuples.of(new MemberBean(response), new UserBean(response.getUser())))
                .map(tuple -> new Member(serviceMediator, tuple.getT1(), tuple.getT2(), guildId))
                .collect(Collectors.toSet());

        Mono<Void> saveOfflinePresences = emptyIfReadOnly(Flux.fromIterable(members)
                .map(Member::getId)
                .map(Snowflake::asLong)
                .filterWhen(id -> serviceMediator.getStateHolder().getPresenceStore()
                        .find(LongLongTuple2.of(guildId, id))
                        .hasElement()
                        .map(identity -> !identity))
                .flatMap(id -> serviceMediator.getStateHolder().getPresenceStore()
                        .save(LongLongTuple2.of(guildId, id), PresenceBean.DEFAULT_OFFLINE))
                .then());

        return addMemberIds
                .and(saveMembers)
                .and(saveUsers)
                .and(saveOfflinePresences)
                .thenReturn(new MemberChunkEvent(serviceMediator.getClient(), guildId, members));
    }

    Mono<MemberUpdateEvent> guildMemberUpdate(DispatchContext<GuildMemberUpdate> context) {
        ServiceMediator serviceMediator = context.getServiceMediator();
        DiscordClient client = serviceMediator.getClient();

        long guildId = context.getDispatch().getGuildId();
        long memberId = context.getDispatch().getUser().getId();

        long[] currentRoles = context.getDispatch().getRoles();
        String currentNick = context.getDispatch().getNick();

        LongLongTuple2 key = LongLongTuple2.of(guildId, memberId);

        Mono<MemberUpdateEvent> update = serviceMediator.getStateHolder().getMemberStore()
                .find(key)
                .flatMap(bean -> {
                    UserBean user = new UserBean(context.getDispatch().getUser());
                    Member old = new Member(serviceMediator, new MemberBean(bean, context.getDispatch()), user,
                            guildId);

                    bean.setNick(currentNick);
                    bean.setRoles(currentRoles);

                    return emptyIfReadOnly(serviceMediator.getStateHolder().getMemberStore().save(key, bean))
                            .thenReturn(new MemberUpdateEvent(client, guildId, memberId, old, currentRoles,
                                    currentNick));
                });

        return update.defaultIfEmpty(new MemberUpdateEvent(client, guildId, memberId, null, currentRoles, currentNick));
    }

    Mono<RoleCreateEvent> guildRoleCreate(DispatchContext<GuildRoleCreate> context) {
        ServiceMediator serviceMediator = context.getServiceMediator();
        DiscordClient client = serviceMediator.getClient();
        long guildId = context.getDispatch().getGuildId();
        RoleBean bean = new RoleBean(context.getDispatch().getRole());
        Role role = new Role(serviceMediator, bean, guildId);

        Mono<Void> addRoleId = serviceMediator.getStateHolder().getGuildStore()
                .find(context.getDispatch().getGuildId())
                .doOnNext(guild -> guild.setRoles(ArrayUtil.add(guild.getRoles(), bean.getId())))
                .flatMap(guild -> serviceMediator.getStateHolder().getGuildStore().save(guild.getId(), guild));

        Mono<Void> saveRole = emptyIfReadOnly(serviceMediator.getStateHolder().getRoleStore().save(bean.getId(), bean));

        return addRoleId
                .and(saveRole)
                .thenReturn(new RoleCreateEvent(client, guildId, role));
    }

    Mono<RoleDeleteEvent> guildRoleDelete(DispatchContext<GuildRoleDelete> context) {
        ServiceMediator serviceMediator = context.getServiceMediator();
        long guildId = context.getDispatch().getGuildId();
        long roleId = context.getDispatch().getRoleId();

        Mono<Void> removeRoleId = serviceMediator.getStateHolder().getGuildStore()
                .find(guildId)
                .doOnNext(guild -> guild.setRoles(ArrayUtil.remove(guild.getRoles(), roleId)))
                .flatMap(guild -> emptyIfReadOnly(serviceMediator.getStateHolder().getGuildStore()
                        .save(guildId, guild)));

        Mono<Void> deleteRole = emptyIfReadOnly(serviceMediator.getStateHolder().getRoleStore()
                .delete(context.getDispatch().getRoleId()));

        Mono<Void> removeRoleFromMembers = serviceMediator.getStateHolder().getGuildStore()
                .find(guildId)
                .flatMapMany(guild -> Flux.fromArray(ArrayUtil.toObject(guild.getMembers())))
                .flatMap(memberId -> serviceMediator.getStateHolder().getMemberStore().find(LongLongTuple2.of(guildId
                        , memberId)).map(member -> Tuples.of(memberId, member)))
                .filter(t -> ArrayUtil.contains(t.getT2().getRoles(), roleId))
                .doOnNext(t -> {
                    MemberBean member = t.getT2();
                    member.setRoles(ArrayUtil.remove(member.getRoles(), roleId));
                })
                .flatMap(t -> emptyIfReadOnly(serviceMediator.getStateHolder().getMemberStore()
                        .save(LongLongTuple2.of(guildId, t.getT1()), t.getT2())))
                .then();


        return serviceMediator.getStateHolder().getRoleStore()
                .find(roleId)
                .flatMap(removeRoleId::thenReturn)
                .flatMap(deleteRole::thenReturn)
                .flatMap(removeRoleFromMembers::thenReturn)
                .map(role -> new RoleDeleteEvent(serviceMediator.getClient(), guildId, roleId,
                        new Role(serviceMediator, role, guildId)))
                .defaultIfEmpty(new RoleDeleteEvent(serviceMediator.getClient(), guildId, roleId, null));
    }

    Mono<RoleUpdateEvent> guildRoleUpdate(DispatchContext<GuildRoleUpdate> context) {
        ServiceMediator serviceMediator = context.getServiceMediator();
        DiscordClient client = serviceMediator.getClient();
        long guildId = context.getDispatch().getGuildId();

        RoleBean bean = new RoleBean(context.getDispatch().getRole());
        Role current = new Role(serviceMediator, bean, guildId);

        Mono<Void> saveNew = emptyIfReadOnly(serviceMediator.getStateHolder().getRoleStore().save(bean.getId(), bean));

        return serviceMediator.getStateHolder().getRoleStore()
                .find(context.getDispatch().getRole().getId())
                .flatMap(saveNew::thenReturn)
                .map(old -> new RoleUpdateEvent(client, current, new Role(serviceMediator, old, guildId)))
                .switchIfEmpty(saveNew.thenReturn(new RoleUpdateEvent(client, current, null)));
    }

    Mono<GuildUpdateEvent> guildUpdate(DispatchContext<GuildUpdate> context) {
        ServiceMediator serviceMediator = context.getServiceMediator();
        DiscordClient client = serviceMediator.getClient();

        long guildId = context.getDispatch().getGuildId();

        Mono<GuildUpdateEvent> update = context.getServiceMediator().getStateHolder().getGuildStore()
                .find(guildId)
                .flatMap(oldBean -> {
                    GuildBean newBean = new GuildBean(oldBean, context.getDispatch());

                    Guild old = new Guild(context.getServiceMediator(), oldBean);
                    Guild current = new Guild(context.getServiceMediator(), newBean);

                    return emptyIfReadOnly(context.getServiceMediator().getStateHolder().getGuildStore()
                            .save(newBean.getId(), newBean))
                            .thenReturn(new GuildUpdateEvent(client, current, old));
                });

        Guild current = new Guild(serviceMediator, new BaseGuildBean(context.getDispatch()));

        return update.defaultIfEmpty(new GuildUpdateEvent(client, current, null));
    }

}
