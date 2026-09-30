package api

import "github.com/gotd/td/tg"

// chatDefaultRightNames is the subset of ChatBannedRights already represented
// by chats.default_banned_rights. Unknown and reserved TL flag bits are rejected
// instead of being acknowledged without persistence.
var chatDefaultRightNames = map[int]string{
	1:  "send_messages",
	2:  "send_media",
	3:  "send_stickers",
	4:  "send_gifs",
	5:  "send_games",
	6:  "send_inline",
	7:  "embed_links",
	8:  "send_polls",
	10: "change_info",
	15: "invite_users",
	17: "pin_messages",
	18: "manage_topics",
	19: "send_photos",
	20: "send_videos",
	21: "send_roundvideos",
	22: "send_audios",
	23: "send_voices",
	24: "send_docs",
	25: "send_plain",
}

func chatDefaultBannedRightsFromTL(rights tg.ChatBannedRights) ([]string, error) {
	if rights.ViewMessages || rights.UntilDate != 0 || rights.EditRank || rights.SendReactions || rights.ManageLinkedPeers {
		return nil, errBannedRightsInvalid
	}

	stored := make([]string, 0, len(chatDefaultRightNames))
	for flag := range 32 {
		if !rights.Flags.Has(flag) {
			continue
		}
		name, ok := chatDefaultRightNames[flag]
		if !ok {
			return nil, errBannedRightsInvalid
		}
		stored = append(stored, name)
	}
	return stored, nil
}

func chatDefaultBannedRightsToTL(rights []string) tg.ChatBannedRights {
	var out tg.ChatBannedRights
	for _, right := range rights {
		switch right {
		case "send_messages":
			out.SendMessages = true
		case "send_media":
			out.SendMedia = true
		case "send_stickers":
			out.SendStickers = true
		case "send_gifs":
			out.SendGifs = true
		case "send_games":
			out.SendGames = true
		case "send_inline":
			out.SendInline = true
		case "embed_links":
			out.EmbedLinks = true
		case "send_polls":
			out.SendPolls = true
		case "change_info":
			out.ChangeInfo = true
		case "invite_users":
			out.InviteUsers = true
		case "pin_messages":
			out.PinMessages = true
		case "manage_topics":
			out.ManageTopics = true
		case "send_photos":
			out.SendPhotos = true
		case "send_videos":
			out.SendVideos = true
		case "send_roundvideos":
			out.SendRoundvideos = true
		case "send_audios":
			out.SendAudios = true
		case "send_voices":
			out.SendVoices = true
		case "send_docs":
			out.SendDocs = true
		case "send_plain":
			out.SendPlain = true
		}
	}
	return out
}
